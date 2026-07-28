package com.example.myproject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.InitException;
import org.yamcs.Processor;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.YamcsServerInstance;
import org.yamcs.buckets.Bucket;
import org.yamcs.cfdp.CfdpService;
import org.yamcs.commanding.CommandingManager;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.filetransfer.FileTransfer;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.TransferOptions;
import org.yamcs.protobuf.EntityInfo;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.RemoteFile;
import org.yamcs.protobuf.TransferDirection;
import org.yamcs.protobuf.TransferState;
import org.yamcs.security.User;
import org.yamcs.xtce.MetaCommand;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.protobuf.Db.Event;

/**
 * A {@link CfdpService} that downlinks by commanding the spacecraft, rather
 * than by sending a CFDP Proxy Put Request.
 *
 * <p><b>Why this exists.</b> Stock {@code CfdpService.startDownload()} — the
 * method behind the "Download" button in the yamcs-web File Transfer dialog —
 * implements ground-initiated downlink as a CFDP Proxy Put Request: a
 * <i>fileless</i> Metadata PDU (zero-length source and destination filenames)
 * carrying a {@code ProxyPutRequest} Message-To-User TLV.
 *
 * <p>F´'s {@code Svc::Ccsds::CfdpManager} does not implement Message-To-User
 * TLVs. Worse, {@code MetadataPdu::fromSerialBuffer} rejects outright any
 * metadata PDU whose source filename is empty:
 *
 * <pre>
 *   // Validate filename is not empty
 *   if (sourceFilenameLength == 0) {
 *       return Fw::FW_DESERIALIZE_SIZE_MISMATCH;   // == 5
 *   }
 * </pre>
 *
 * <p>So the proxy request is dropped on the floor: F´ logs
 * {@code FailMetadataPduDeserialization ... status 5} and never acknowledges.
 * YAMCS then exhausts its EOF retries, the sender fault handler fires, and the
 * transfer is left parked in the pending map. Because a fileless transfer's
 * {@code getRemotePath()} is null and {@code CfdpService.isRunning()} counts
 * PAUSED/CANCELLING, the duplicate-path scan in {@code startUpload()} then
 * NPEs — taking down <i>every subsequent upload</i> until YAMCS restarts.
 * One click of the Download button bricked file transfer for the session.
 *
 * <p><b>What this does instead.</b> Override {@code startDownload()} to
 * synthesize an F´ {@code CfdpManager.SendFile} command. The spacecraft then
 * originates an ordinary CFDP transaction in the ground's direction. Its
 * Metadata / FileData / EOF PDUs arrive over the normal inbound path
 * ({@code CfdpBridgeService} → {@code cfdp_in} → inherited {@code processPdu})
 * and the inherited receiver reassembles them into the local entity's bucket.
 * Nothing about uplink, PDU handling, or reassembly changes — this class only
 * replaces how a downlink is <i>initiated</i>.
 *
 * <p>The UI's "Reliable" checkbox drives the CFDP class in both directions:
 * checked maps to {@code CLASS_2} (acknowledged), unchecked to {@code CLASS_1}
 * (unacknowledged), matching what the same checkbox already does for uplink.
 *
 * <p><b>Note on the returned transfer.</b> {@code startDownload()} must return
 * a {@link FileTransfer} synchronously, but the real transfer object does not
 * exist until the spacecraft's first PDU arrives and the inherited receiver
 * builds a {@code CfdpIncomingTransfer}. The object returned here is therefore
 * a short-lived receipt for the command dispatch; it is deliberately not
 * registered in the parent's transfer table, so the transfer list shows the
 * single real incoming transfer rather than a duplicate placeholder row.
 *
 * <p><b>Configuration.</b> Substitute for {@code org.yamcs.cfdp.CfdpService},
 * keeping every existing CFDP arg. Two settings need care:
 * <pre>
 *   - class: com.example.myproject.FprimeCfdpService
 *     name: FprimeFilePacketService
 *     args:
 *       # REQUIRED: CfdpService compares this against its own runtime class
 *       # name to decide whether it is its own file-listing service. Left at
 *       # the default ("org.yamcs.cfdp.CfdpService") the comparison fails for
 *       # any subclass and YAMCS instantiates a second, detached CfdpService.
 *       fileListingServiceClassName: com.example.myproject.FprimeCfdpService
 *
 *       sendFileCommand: ""      # auto-discovered by suffix when blank
 *       downlinkChannelId: 0
 *       downlinkKeep: KEEP       # or DELETE, to remove the file after sending
 *       downlinkPriority: 0
 * </pre>
 *
 * <p>Remote file listing is unavailable regardless: directory listings also
 * travel as Message-To-User TLVs. Operators type the spacecraft path into the
 * dialog's "Remote filename" field, which is what the Download button reads.
 */
public class FprimeCfdpService extends CfdpService {

    private static final Logger LOG = LoggerFactory.getLogger(FprimeCfdpService.class);

    // Argument names on Svc.Ccsds.Cfdp.CfdpManager.SendFile. See the F´
    // component's commands FPP; these are stable across the MDB export.
    private static final String ARG_CHANNEL = "channelId";
    private static final String ARG_DEST_ID = "destId";
    private static final String ARG_CLASS = "cfdpClass";
    private static final String ARG_KEEP = "keep";
    private static final String ARG_PRIORITY = "priority";
    private static final String ARG_SOURCE = "sourceFileName";
    private static final String ARG_DEST = "destFileName";

    // Enum labels as exported to the MDB. CLASS_2 is acknowledged.
    private static final String CLASS_ACKNOWLEDGED = "CLASS_2";
    private static final String CLASS_UNACKNOWLEDGED = "CLASS_1";

    // Shim transfer ids live far above the Yarch sequence the parent draws
    // real transfer ids from, so a receipt can never collide with a real
    // transfer in a client that caches by id.
    private static final long SHIM_ID_BASE = 1_000_000_000L;

    private final AtomicLong shimIdSeq = new AtomicLong(SHIM_ID_BASE);

    private String sendFileCommandName;
    private int downlinkChannelId;
    private String downlinkKeep;
    private int downlinkPriority;

    private String listDirectoryCommandName;
    private String listDirArg;
    private String eventStreamName;
    private int maxRemotePathLength;
    private long listingTimeoutMs;
    private String remoteRoot;
    private ScheduledExecutorService listingSweeper;

    // Directory listings in progress, keyed by the directory path F´ echoes
    // back in every listing event.
    private final Map<String, ListingAccumulator> inProgressListings = new ConcurrentHashMap<>();

    // Resolved lazily on first downlink rather than in doStart(), so this
    // service does not depend on starting after ProcessorCreatorService.
    private volatile boolean commandingResolved;
    private CommandingManager commandingManager;
    private MetaCommand sendFileCommand;
    private MetaCommand listDirectoryCommand;
    private User systemUser;

    @Override
    public Spec getSpec() {
        Spec spec = super.getSpec();
        spec.addOption("sendFileCommand", OptionType.STRING).withDefault("");
        spec.addOption("downlinkChannelId", OptionType.INTEGER).withDefault(0);
        spec.addOption("downlinkKeep", OptionType.STRING).withDefault("KEEP");
        spec.addOption("downlinkPriority", OptionType.INTEGER).withDefault(0);
        spec.addOption("listDirectoryCommand", OptionType.STRING).withDefault("");
        spec.addOption("listDirArg", OptionType.STRING).withDefault("dirName");
        spec.addOption("eventStream", OptionType.STRING).withDefault("events_realtime");
        // Directory the remote file browser opens at, and the base that
        // browser-relative paths resolve against. "/" browses the whole
        // spacecraft filesystem; "." is the flight software's own working
        // directory, which is where uploads with a relative destination land.
        spec.addOption("remoteRoot", OptionType.STRING).withDefault("/");
        // FW_CMD_STRING_MAX_SIZE is 40 in default/config/FpConstants.fpp, and a
        // command string argument must fit within it. Raise this only if the
        // flight software was rebuilt with a larger constant.
        spec.addOption("maxRemotePathLength", OptionType.INTEGER).withDefault(39);
        spec.addOption("listingTimeoutMs", OptionType.INTEGER).withDefault(60000);
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config) throws InitException {
        super.init(yamcsInstance, serviceName, config);
        this.sendFileCommandName = config.getString("sendFileCommand", "");
        this.downlinkChannelId = config.getInt("downlinkChannelId", 0);
        this.downlinkKeep = config.getString("downlinkKeep", "KEEP");
        this.downlinkPriority = config.getInt("downlinkPriority", 0);
        this.listDirectoryCommandName = config.getString("listDirectoryCommand", "");
        this.listDirArg = config.getString("listDirArg", "dirName");
        this.eventStreamName = config.getString("eventStream", "events_realtime");
        this.maxRemotePathLength = config.getInt("maxRemotePathLength", 39);
        this.listingTimeoutMs = config.getLong("listingTimeoutMs", 60000L);
        this.remoteRoot = config.getString("remoteRoot", "/");

        LOG.info("FprimeCfdpService init: downlink via SendFile command (channel={} keep={} priority={})",
                downlinkChannelId, downlinkKeep, downlinkPriority);
    }

    @Override
    protected void doStart() {
        super.doStart();
        // Directory listings are assembled from F´ FileManager events.
        YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);
        Stream events = ydb.getStream(eventStreamName);
        if (events == null) {
            LOG.warn("Stream '{}' not found; the remote file browser will stay empty", eventStreamName);
        } else {
            events.addSubscriber(new ListingEventSubscriber());
            LOG.info("Subscribed to {} for remote directory listings", eventStreamName);
        }

        listingSweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "FprimeCfdpService-listing-sweeper");
            t.setDaemon(true);
            return t;
        });
        listingSweeper.scheduleWithFixedDelay(this::sweepStaleListings, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() {
        if (listingSweeper != null) {
            listingSweeper.shutdownNow();
        }
        super.doStop();
    }

    /**
     * Command the spacecraft to send us a file, in place of the inherited
     * Proxy Put Request.
     *
     * @param sourceEntity      remote entity name; the spacecraft holding the file
     * @param sourcePath        path of the file on the spacecraft
     * @param destinationEntity local entity name; becomes SendFile's destId
     * @param bucket            bucket the inherited receiver will save into
     * @param objectName        object name to store under; becomes destFileName
     * @param options           {@code reliable} selects CLASS_2 over CLASS_1
     */
    @Override
    public FileTransfer startDownload(String sourceEntity, String sourcePath, String destinationEntity,
            Bucket bucket, String objectName, TransferOptions options) throws InvalidRequestException {

        if (sourcePath == null || sourcePath.isBlank()) {
            throw new InvalidRequestException(
                    "A remote path is required: type the file's path on the spacecraft into"
                            + " 'Remote filename'. F´ does not support CFDP directory listings,"
                            + " so the remote file browser cannot populate it for you.");
        }

        // A path picked from the remote browser arrives breadcrumb-relative
        // ("private/tmp/x"), so re-anchor it the same way listings do.
        sourcePath = resolveRemotePath(sourcePath);

        if (sourcePath.length() > maxRemotePathLength) {
            throw new InvalidRequestException(String.format(
                    "Remote path is %d characters; F´ accepts at most %d in a command"
                            + " argument (FW_CMD_STRING_MAX_SIZE). Use a shorter path"
                            + " on the spacecraft, e.g. under /tmp.",
                    sourcePath.length(), maxRemotePathLength));
        }

        // Mirror the parent's convention: an omitted object name defaults to
        // the basename of the remote path.
        if (objectName == null || objectName.isBlank()) {
            String[] split = sourcePath.split("[\\\\/]");
            objectName = split[split.length - 1];
        }

        long localEntityId = entityIdByName(getLocalEntities(), destinationEntity, "local");
        long remoteEntityId = entityIdByName(getRemoteEntities(), sourceEntity, "remote");

        // The same "Reliable" checkbox that selects the uplink transmission
        // mode selects the downlink CFDP class.
        String cfdpClass = options != null && options.isReliable()
                ? CLASS_ACKNOWLEDGED
                : CLASS_UNACKNOWLEDGED;

        resolveCommanding();

        long shimId = shimIdSeq.incrementAndGet();

        Map<String, Object> args = new HashMap<>();
        args.put(ARG_CHANNEL, downlinkChannelId);
        args.put(ARG_DEST_ID, localEntityId);
        args.put(ARG_CLASS, cfdpClass);
        args.put(ARG_KEEP, downlinkKeep);
        args.put(ARG_PRIORITY, downlinkPriority);
        args.put(ARG_SOURCE, sourcePath);
        args.put(ARG_DEST, objectName);

        try {
            PreparedCommand pc = commandingManager.buildCommand(
                    sendFileCommand, args, "FprimeCfdpService",
                    (int) (shimId & 0x7FFFFFFF), systemUser);
            commandingManager.sendCommand(systemUser, pc);
        } catch (Exception e) {
            LOG.error("Downlink dispatch failed: {} -> {}/{}", sourcePath, bucket.getName(), objectName, e);
            throw new InvalidRequestException("Failed to command " + sendFileCommandName + ": " + e.getMessage());
        }

        LOG.info("Downlink commanded: {} ({}) {} -> bucket {}/{}; awaiting spacecraft PDUs",
                sendFileCommandName, cfdpClass, sourcePath, bucket.getName(), objectName);

        return new DownlinkRequestReceipt(shimId, bucket.getName(), objectName, sourcePath,
                localEntityId, remoteEntityId, CLASS_ACKNOWLEDGED.equals(cfdpClass));
    }

    /**
     * Populate the remote file browser by commanding an F´ directory listing,
     * in place of the inherited CFDP {@code DirectoryListingRequest} TLV.
     *
     * <p>This kicks off {@code FileManager.ListDirectory} and returns
     * immediately. F´ then emits one {@code DirectoryListing} /
     * {@code DirectoryListingSubdir} event per entry (throttled to roughly one
     * per second) and finally {@code ListDirectorySucceeded}. Those events are
     * gathered by {@link ListingEventSubscriber}, which hands the finished
     * listing to the inherited {@code saveFileList} and monitor fan-out — so
     * persistence, retrieval via {@code getFileList}, and the UI's live update
     * all work exactly as they do for a stock CFDP listing.
     */
    @Override
    public void fetchFileList(String source, String destination, String remotePath, Map<String, Object> options) {
        // Two different strings are needed here, and conflating them means the
        // listing is stored under a key the UI never looks up:
        //
        //   storageKey - must match the inherited getFileList() exactly, which
        //                does remotePath.replaceFirst("/*$", ""). That maps
        //                both "" and "/" to the EMPTY string, so root is keyed
        //                as "", not "/".
        //   dirToList  - what F´ is asked to list. F´ echoes this value back as
        //                dirName in every listing event, so it is also the key
        //                the accumulator map uses.
        //
        String storageKey = remotePath == null ? "" : remotePath.replaceFirst("/*$", "");
        String dirToList = resolveRemotePath(storageKey);

        String remoteEntityName = destination == null || destination.isBlank()
                ? getRemoteEntities().get(0).getName()
                : destination;

        // F´ command string arguments are Fw::CmdStringArg, i.e.
        // StringTemplate<FW_CMD_STRING_MAX_SIZE>, and that constant is 40.
        // Anything longer is rejected by the command dispatcher with a bare
        // FORMAT_ERROR that says nothing about why. Note the MDB advertises
        // dirName as 240 characters, which is misleading -- the flight-side
        // limit is what actually applies. Fail loudly and record the reason so
        // the UI shows it instead of an unexplained empty directory.
        if (dirToList.length() > maxRemotePathLength) {
            String reason = String.format(
                    "Path is %d characters; F´ accepts at most %d in a command argument"
                            + " (FW_CMD_STRING_MAX_SIZE). Use a shorter path.",
                    dirToList.length(), maxRemotePathLength);
            LOG.warn("fetchFileList({}): {}", dirToList, reason);
            publishFailedListing(storageKey, remoteEntityName, reason);
            return;
        }

        try {
            resolveCommanding();
        } catch (InvalidRequestException e) {
            LOG.warn("fetchFileList({}): {}", dirToList, e.getMessage());
            return;
        }
        if (listDirectoryCommand == null) {
            LOG.warn("fetchFileList({}): no ListDirectory command available", dirToList);
            return;
        }

        // Keyed by the path F´ echoes back; carries the storage key so the
        // finished listing is saved where getFileList() will look for it.
        inProgressListings.put(dirToList, new ListingAccumulator(storageKey, remoteEntityName));

        try {
            Map<String, Object> args = new HashMap<>();
            args.put(listDirArg, dirToList);
            PreparedCommand pc = commandingManager.buildCommand(
                    listDirectoryCommand, args, "FprimeCfdpService-listing", 0, systemUser);
            commandingManager.sendCommand(systemUser, pc);
            LOG.info("Requested F´ listing of {} (stored under key '{}')", dirToList, storageKey);
        } catch (Exception e) {
            LOG.error("fetchFileList({}): failed to dispatch {}", dirToList, listDirectoryCommandName, e);
            inProgressListings.remove(dirToList);
        }
    }

    /**
     * Collects entries for one in-flight directory listing, then converts them
     * into a {@link ListFilesResponse} when the terminal event arrives.
     */
    private static final class ListingAccumulator {
        /** Key the inherited getFileList() will look this listing up by. */
        private final String storageKey;
        private final String destination;
        private final List<RemoteFile> entries = new ArrayList<>();
        private final long startedAt = System.currentTimeMillis();

        ListingAccumulator(String storageKey, String destination) {
            this.storageKey = storageKey;
            this.destination = destination;
        }

        synchronized void add(String name, long size, boolean isDirectory) {
            entries.add(RemoteFile.newBuilder()
                    .setName(name)
                    .setIsDirectory(isDirectory)
                    .setSize(size)
                    .build());
        }

        synchronized ListFilesResponse build(String state) {
            return build(state, null);
        }

        synchronized ListFilesResponse build(String state, String progressMessage) {
            long now = System.currentTimeMillis();
            ListFilesResponse.Builder b = ListFilesResponse.newBuilder()
                    .setRemotePath(storageKey)
                    .setDestination(destination)
                    .setState(state)
                    .setListTime(Timestamp.newBuilder()
                            .setSeconds(now / 1000)
                            .setNanos((int) ((now % 1000) * 1_000_000))
                            .build())
                    .addAllFiles(entries);
            if (progressMessage != null) {
                b.setProgressMessage(progressMessage);
            }
            return b.build();
        }
    }

    /**
     * Drives the listing state machine from F´ FileManager events.
     *
     * <p>Reads the structured argument map rather than parsing the rendered
     * message: fprime-yamcs-events publishes the decoded arguments in
     * {@code Event.extra}, e.g.
     * {@code {dirName=/tmp, fileName=a.txt, fileSize=50}}.
     *
     * <p>Note the stream carries the internal {@code Db.Event} protobuf, not
     * the external {@code org.yamcs.protobuf.Event} API type. They share field
     * names but are wire-incompatible classes.
     */
    private final class ListingEventSubscriber implements StreamSubscriber {
        @Override
        public void onTuple(Stream stream, Tuple tuple) {
            Object body = tuple.getColumn("body");
            if (!(body instanceof Event)) {
                return;
            }
            Event evt = (Event) body;
            String type = evt.getType();
            if (type == null) {
                return;
            }
            int dot = type.lastIndexOf('.');
            if (dot >= 0) {
                type = type.substring(dot + 1);
            }

            Map<String, String> extra = evt.getExtraMap();
            if (extra == null || extra.isEmpty()) {
                return;
            }
            String dir = extra.get("dirName");
            if (dir == null) {
                return;
            }

            try {
                switch (type) {
                case "DirectoryListing": {
                    ListingAccumulator acc = inProgressListings.get(dir);
                    String file = extra.get("fileName");
                    String size = extra.get("fileSize");
                    if (acc != null && file != null && size != null) {
                        acc.add(file, Long.parseLong(size), false);
                    }
                    break;
                }
                case "DirectoryListingSubdir": {
                    ListingAccumulator acc = inProgressListings.get(dir);
                    String subdir = extra.get("subdirName");
                    if (acc != null && subdir != null) {
                        acc.add(subdir, 0, true);
                    }
                    break;
                }
                case "ListDirectorySucceeded":
                    completeListing(dir, "COMPLETED");
                    break;
                case "ListDirectoryError":
                    completeListing(dir, "FAILED");
                    break;
                default:
                    // not a listing event
                }
            } catch (Exception e) {
                LOG.warn("Error handling listing event type={} dir={}", type, dir, e);
            }
        }

        @Override
        public void streamClosed(Stream stream) {
            // nothing to do; the service is stopping
        }
    }

    /**
     * Finish a listing and publish it through the inherited persistence and
     * monitor fan-out, so it reaches both {@code getFileList} and any live UI
     * subscriber.
     */
    private void completeListing(String dir, String state) {
        ListingAccumulator acc = inProgressListings.remove(dir);
        if (acc == null) {
            return;  // not a listing we started
        }
        ListFilesResponse response = acc.build(state);
        saveFileList(response);
        notifyRemoteFileListMonitors(response);
        LOG.info("Listing of {} {}: {} entries", dir, state, response.getFilesCount());
    }

    /**
     * Publish an immediately-failed listing carrying the reason, for requests
     * rejected before any command is sent. Without this the UI just shows an
     * empty directory and gives the operator nothing to act on.
     */
    private void publishFailedListing(String storageKey, String destination, String reason) {
        ListFilesResponse response = new ListingAccumulator(storageKey, destination)
                .build("FAILED", reason);
        saveFileList(response);
        notifyRemoteFileListMonitors(response);
    }

    /**
     * Drop listings that never reached a terminal event.
     *
     * <p>{@code ListDirectoryStarted} is not guaranteed to be followed by
     * {@code ListDirectorySucceeded} or {@code ListDirectoryError} -- a command
     * rejected by the dispatcher (bad path, over-long argument) produces
     * neither, and without this the accumulator would sit in the map forever.
     */
    private void sweepStaleListings() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ListingAccumulator> e : new ArrayList<>(inProgressListings.entrySet())) {
            ListingAccumulator acc = e.getValue();
            long age = now - acc.startedAt;
            if (age < listingTimeoutMs) {
                continue;
            }
            // Lost the race to a terminal event? Then it is already handled.
            if (!inProgressListings.remove(e.getKey(), acc)) {
                continue;
            }
            String reason = "no response from F´ after " + age + " ms";
            LOG.warn("Listing of {} timed out: {}", e.getKey(), reason);
            ListFilesResponse response = acc.build("FAILED", reason);
            saveFileList(response);
            notifyRemoteFileListMonitors(response);
        }
    }

    /**
     * Resolve the commanding manager, the SendFile MetaCommand and the system
     * user, on first use. When {@code sendFileCommand} is left blank the
     * command is discovered by qualified-name suffix, which keeps the config
     * portable across deployments that nest the CFDP subtopology differently.
     */
    private synchronized void resolveCommanding() throws InvalidRequestException {
        if (commandingResolved) {
            return;
        }
        YamcsServerInstance ysi = YamcsServer.getServer().getInstance(yamcsInstance);
        Processor processor = ysi == null ? null : ysi.getFirstProcessor();
        if (processor == null) {
            throw new InvalidRequestException("No processor available to issue the downlink command");
        }
        commandingManager = processor.getCommandingManager();

        String name = sendFileCommandName;
        if (name == null || name.isEmpty()) {
            name = discoverCommand(processor, "/SendFile", "cfdpmanager");
            if (name == null) {
                throw new InvalidRequestException(
                        "Could not auto-discover a CfdpManager SendFile command in the MDB;"
                                + " set the 'sendFileCommand' arg explicitly");
            }
            LOG.info("Auto-discovered downlink command: {}", name);
        }

        sendFileCommand = processor.getMdb().getMetaCommand(name);
        if (sendFileCommand == null) {
            throw new InvalidRequestException("Downlink command '" + name + "' not found in the MDB");
        }
        sendFileCommandName = name;

        // Listing is optional: a deployment without FileManager still gets
        // working uplink and downlink, just no remote file browser.
        String listName = listDirectoryCommandName;
        if (listName == null || listName.isEmpty()) {
            listName = discoverCommand(processor, "/ListDirectory", "filemanager");
        }
        if (listName != null) {
            listDirectoryCommand = processor.getMdb().getMetaCommand(listName);
            listDirectoryCommandName = listName;
        }
        if (listDirectoryCommand == null) {
            LOG.warn("No ListDirectory command found in the MDB; the remote file browser will stay empty");
        } else {
            LOG.info("Directory listings via: {}", listDirectoryCommandName);
        }

        systemUser = YamcsServer.getServer().getSecurityStore().getSystemUser();
        commandingResolved = true;
    }

    /**
     * Find a command in the MDB by qualified-name suffix.
     *
     * <p>Matching is case-insensitive on purpose: F´ exports the <i>instance</i>
     * name, which is conventionally lower-camel ({@code cfdpManager},
     * {@code fileManager}), while the component type is upper-camel. Matching
     * case-sensitively against the component name silently finds nothing.
     *
     * @param suffix         required qualified-name suffix, e.g. {@code "/SendFile"}
     * @param preferContains when several commands share the suffix, prefer one
     *                       whose qualified name contains this (lower-case) fragment
     * @return the qualified name, or null if nothing matched
     */
    /**
     * Turn a path as the web UI expresses it into one F´ can act on.
     *
     * <p>yamcs-web builds paths breadcrumb-style with no leading slash: root is
     * {@code ""}, one level down is {@code "private"}, then
     * {@code "private/tmp"}. F´ resolves a relative path against the flight
     * software's own working directory, so those have to be re-anchored against
     * {@link #remoteRoot} or they silently resolve somewhere unintended.
     *
     * <p>A path the operator typed with a leading slash is already absolute and
     * is passed through untouched, so {@code remoteRoot} constrains where
     * browsing <i>starts</i> without preventing anyone from going elsewhere.
     */
    private String resolveRemotePath(String browserPath) {
        if (browserPath == null || browserPath.isEmpty()) {
            return remoteRoot;
        }
        if (browserPath.startsWith("/")) {
            return browserPath;
        }
        return remoteRoot.endsWith("/")
                ? remoteRoot + browserPath
                : remoteRoot + "/" + browserPath;
    }

    private static String discoverCommand(Processor processor, String suffix, String preferContains) {
        String fallback = null;
        for (MetaCommand cmd : processor.getMdb().getMetaCommands()) {
            String qn = cmd.getQualifiedName();
            String lower = qn.toLowerCase();
            if (!lower.endsWith(suffix.toLowerCase())) {
                continue;
            }
            if (lower.contains(preferContains)) {
                return qn;
            }
            if (fallback == null) {
                fallback = qn;
            }
        }
        return fallback;
    }

    private static long entityIdByName(List<EntityInfo> entities, String name, String which)
            throws InvalidRequestException {
        if (entities.isEmpty()) {
            throw new InvalidRequestException("No " + which + " entities configured");
        }
        if (name == null || name.isBlank()) {
            return entities.get(0).getId();
        }
        for (EntityInfo e : entities) {
            if (e.getName().equals(name)) {
                return e.getId();
            }
        }
        throw new InvalidRequestException("Unknown " + which + " entity '" + name + "'");
    }

    /**
     * Synchronous receipt for a dispatched downlink command.
     *
     * <p>Reports RUNNING with an unknown total size: the spacecraft has been
     * asked for the file but has not yet said how large it is. Progress and
     * completion are carried by the {@code CfdpIncomingTransfer} the inherited
     * receiver creates once PDUs start arriving; this object is not updated
     * and is not registered with the parent.
     */
    private static final class DownlinkRequestReceipt implements FileTransfer {
        private final long id;
        private final String bucketName;
        private final String objectName;
        private final String remotePath;
        private final long localEntityId;
        private final long remoteEntityId;
        private final boolean reliable;
        private final long time = System.currentTimeMillis();

        DownlinkRequestReceipt(long id, String bucketName, String objectName, String remotePath,
                long localEntityId, long remoteEntityId, boolean reliable) {
            this.id = id;
            this.bucketName = bucketName;
            this.objectName = objectName;
            this.remotePath = remotePath;
            this.localEntityId = localEntityId;
            this.remoteEntityId = remoteEntityId;
            this.reliable = reliable;
        }

        @Override public long getId() { return id; }
        @Override public String getBucketName() { return bucketName; }
        @Override public String getObjectName() { return objectName; }
        @Override public String getRemotePath() { return remotePath; }
        @Override public Long getLocalEntityId() { return localEntityId; }
        @Override public Long getRemoteEntityId() { return remoteEntityId; }
        @Override public TransferDirection getDirection() { return TransferDirection.DOWNLOAD; }
        @Override public long getTotalSize() { return -1; }
        @Override public long getTransferredSize() { return 0; }
        @Override public TransferState getTransferState() { return TransferState.RUNNING; }
        @Override public boolean isReliable() { return reliable; }
        @Override public String getFailuredReason() { return null; }
        @Override public long getCreationTime() { return time; }
        @Override public long getStartTime() { return time; }
        @Override public String getTransferType() { return "DOWNLINK REQUEST (SendFile)"; }
        @Override public boolean pausable() { return false; }
        @Override public boolean cancellable() { return false; }
    }
}
