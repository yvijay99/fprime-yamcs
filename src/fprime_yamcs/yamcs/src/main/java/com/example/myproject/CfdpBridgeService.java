package com.example.myproject;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.AbstractYamcsService;
import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

/**
 * Bridge service that subscribes to tm_realtime, filters CFDP packets by APID,
 * strips CCSDS headers, and emits reformatted tuples to cfdp_in with the
 * column structure expected by YAMCS CfdpService (gentime, seqNum, rectime, pdu).
 * <p>
 * This bridge is necessary because:
 * <ul>
 *   <li>Packet preprocessors emit TmPacket objects that become tuples with a "packet" column</li>
 *   <li>CfdpService expects tuples with a "pdu" column containing raw CFDP PDU bytes</li>
 *   <li>There's no built-in way to rename/transform columns between streams</li>
 * </ul>
 * <p>
 * Configuration in yamcs.fprime-project.yaml:
 * <pre>
 * services:
 *   - class: com.example.myproject.CfdpBridgeService
 *     args:
 *       inStream: tm_realtime
 *       outStream: cfdp_in
 *       fileApid: 3
 * </pre>
 */
public class CfdpBridgeService extends AbstractYamcsService implements StreamSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(CfdpBridgeService.class);

    private static final int CCSDS_PRIMARY_HEADER_SIZE = 6;
    private static final int FW_PACKET_DESCRIPTOR_SIZE = 2;
    private static final int DEFAULT_FILE_APID = 3;

    private String inStreamName;
    private String outStreamName;
    private int fileApid;

    private Stream inStream;
    private Stream outStream;

    @Override
    public Spec getSpec() {
        Spec spec = new Spec();
        spec.addOption("inStream", OptionType.STRING).withDefault("tm_realtime");
        spec.addOption("outStream", OptionType.STRING).withDefault("cfdp_in");
        spec.addOption("fileApid", OptionType.INTEGER).withDefault(DEFAULT_FILE_APID);
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config) throws InitException {
        super.init(yamcsInstance, serviceName, config);
        this.inStreamName = config.getString("inStream", "tm_realtime");
        this.outStreamName = config.getString("outStream", "cfdp_in");
        this.fileApid = config.getInt("fileApid", DEFAULT_FILE_APID);

        LOG.info("CfdpBridgeService init: inStream={} outStream={} fileApid={}",
                inStreamName, outStreamName, fileApid);
    }

    @Override
    protected void doStart() {
        try {
            YarchDatabaseInstance yarch = YarchDatabase.getInstance(yamcsInstance);

            this.inStream = yarch.getStream(inStreamName);
            if (this.inStream == null) {
                notifyFailed(new IllegalStateException("Input stream not found: " + inStreamName));
                return;
            }

            this.outStream = yarch.getStream(outStreamName);
            if (this.outStream == null) {
                notifyFailed(new IllegalStateException("Output stream not found: " + outStreamName));
                return;
            }

            this.inStream.addSubscriber(this);

            LOG.info("CfdpBridgeService started: {} -> {}", inStreamName, outStreamName);
            notifyStarted();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        if (inStream != null) {
            inStream.removeSubscriber(this);
        }
        notifyStopped();
    }

    @Override
    public void onTuple(Stream stream, Tuple tuple) {
        // Extract the raw packet bytes from the tm_realtime tuple
        Object packetCol = tuple.getColumn("packet");
        if (!(packetCol instanceof byte[])) {
            return;
        }
        byte[] bytes = (byte[]) packetCol;

        if (bytes.length < CCSDS_PRIMARY_HEADER_SIZE) {
            return;
        }

        // Extract APID from CCSDS primary header
        int packetId = ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
        int apid = packetId & 0x07FF;

        // Only process packets with the configured file APID
        if (apid != fileApid) {
            return;
        }

        // Extract packet data length from CCSDS header (bytes 4-5)
        int packetDataLength = ((bytes[4] & 0xFF) << 8) | (bytes[5] & 0xFF);
        int expectedTotalLength = CCSDS_PRIMARY_HEADER_SIZE + packetDataLength + 1;

        if (bytes.length < expectedTotalLength) {
            LOG.warn("CFDP packet truncated. Expected: {}, received: {}",
                    expectedTotalLength, bytes.length);
            return;
        }

        // Strip CCSDS primary header and the F' packet descriptor to get raw CFDP PDU.
        // CfdpManager writes a 2-byte FW_PACKET_FILE descriptor at the start of every
        // outgoing PDU buffer, so the PDU itself begins after it.
        int pduStart = CCSDS_PRIMARY_HEADER_SIZE + FW_PACKET_DESCRIPTOR_SIZE;
        if (expectedTotalLength <= pduStart) {
            LOG.warn("CFDP packet too short to contain a PDU. Length: {}", expectedTotalLength);
            return;
        }
        byte[] pdu = Arrays.copyOfRange(bytes, pduStart, expectedTotalLength);

        // Extract sequence count for the tuple
        int seqCount = ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        int sequenceNumber = seqCount & 0x3FFF;

        // Get timestamps from the original tuple
        Long gentime = tuple.hasColumn("gentime") ? (Long) tuple.getColumn("gentime") : null;
        Long rectime = tuple.hasColumn("rectime") ? (Long) tuple.getColumn("rectime") : null;

        // If timestamps are missing, use current time
        long now = System.currentTimeMillis();
        if (gentime == null) {
            gentime = now;
        }
        if (rectime == null) {
            rectime = now;
        }

        // Create output tuple with the structure expected by CfdpService:
        // gentime (TIMESTAMP), seqNum (INT), rectime (TIMESTAMP), pdu (BINARY)
        Tuple cfdpTuple = new Tuple(outStream.getDefinition(), new Object[] {
            gentime,         // gentime
            sequenceNumber,  // seqNum
            rectime,         // rectime
            pdu              // pdu (byte[])
        });

        // Emit to cfdp_in stream
        outStream.emitTuple(cfdpTuple);

        LOG.debug("Forwarded CFDP packet: APID={}, seq={}, pduLen={}", apid, sequenceNumber, pdu.length);
    }

    @Override
    public void streamClosed(Stream stream) {
        LOG.info("Stream {} closed", stream.getName());
    }
}
