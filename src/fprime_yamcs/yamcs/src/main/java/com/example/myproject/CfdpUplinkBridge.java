package com.example.myproject;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.AbstractYamcsService;
import org.yamcs.InitException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.YConfiguration;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.protobuf.Commanding.CommandId;
import org.yamcs.tctm.Link;
import org.yamcs.tctm.ccsds.TcPacketHandler;
import org.yamcs.YamcsServer;
import org.yamcs.YamcsServerInstance;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

/**
 * CFDP Uplink Bridge - Sends CFDP PDUs from cfdp_out to spacecraft via TC link.
 * <p>
 * This is the outgoing counterpart to CfdpBridgeService:
 * <ul>
 *   <li>CfdpBridgeService: tm_realtime (CCSDS) → cfdp_in (PDU tuples)</li>
 *   <li>CfdpUplinkBridge: cfdp_out (PDU tuples) → TC link (CCSDS)</li>
 * </ul>
 * <p>
 * Configuration in yamcs.fprime-project.yaml:
 * <pre>
 * services:
 *   - class: com.example.myproject.CfdpUplinkBridge
 *     args:
 *       inStream: cfdp_out
 *       uplinkLink: UDP_TC_OUT.vc1
 *       fileApid: 3
 * </pre>
 */
public class CfdpUplinkBridge extends AbstractYamcsService implements StreamSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(CfdpUplinkBridge.class);

    private static final int CCSDS_PRIMARY_HEADER_SIZE = 6;
    private static final int FW_PACKET_DESCRIPTOR_SIZE = 2;
    private static final int FW_PACKET_FILE_DESCRIPTOR = 0x0003; // F' ComPacket descriptor for file packets
    private static final int DEFAULT_FILE_APID = 3;

    private String inStreamName;
    private String uplinkLinkName;
    private int fileApid;

    private Stream inStream;
    private TcPacketHandler uplinkLink;
    private int sequenceCount = 0;

    @Override
    public Spec getSpec() {
        Spec spec = new Spec();
        spec.addOption("inStream", OptionType.STRING).withDefault("cfdp_out");
        spec.addOption("uplinkLink", OptionType.STRING).withDefault("UDP_TC_OUT.vc1");
        spec.addOption("fileApid", OptionType.INTEGER).withDefault(DEFAULT_FILE_APID);
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String serviceName, YConfiguration config) throws InitException {
        super.init(yamcsInstance, serviceName, config);
        this.inStreamName = config.getString("inStream", "cfdp_out");
        this.uplinkLinkName = config.getString("uplinkLink", "UDP_TC_OUT.vc1");
        this.fileApid = config.getInt("fileApid", DEFAULT_FILE_APID);

        LOG.info("CfdpUplinkBridge init: inStream={} uplinkLink={} fileApid={}",
                inStreamName, uplinkLinkName, fileApid);
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

            // Resolve TC link for uplink
            YamcsServerInstance instance = YamcsServer.getServer().getInstance(yamcsInstance);
            Link link = instance.getLinkManager().getLink(uplinkLinkName);
            if (!(link instanceof TcPacketHandler)) {
                String what = link == null ? "not found"
                        : "is " + link.getClass().getSimpleName() + ", not TcPacketHandler";
                notifyFailed(new IllegalStateException(
                        "Uplink link '" + uplinkLinkName + "' " + what));
                return;
            }
            this.uplinkLink = (TcPacketHandler) link;

            this.inStream.addSubscriber(this);

            LOG.info("CfdpUplinkBridge started: {} -> {}", inStreamName, uplinkLinkName);
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
        // Extract PDU from cfdp_out tuple
        // Expected columns: gentime, seqNum, pdu
        Object pduCol = tuple.getColumn("pdu");
        if (!(pduCol instanceof byte[])) {
            LOG.warn("cfdp_out tuple missing 'pdu' column or wrong type");
            return;
        }
        byte[] pdu = (byte[]) pduCol;

        try {
            // Wrap PDU in CCSDS space packet
            byte[] ccsdsPacket = buildSpacePacket(pdu, sequenceCount++);

            // Create synthetic PreparedCommand and send via TC link
            CommandId cmdId = CommandId.newBuilder()
                    .setGenerationTime(System.currentTimeMillis())
                    .setOrigin("CfdpUplinkBridge")
                    .setSequenceNumber(sequenceCount)
                    .setCommandName("CfdpUplinkBridge/uplinkCfdpPdu")
                    .build();
            PreparedCommand pc = new PreparedCommand(cmdId);
            pc.setBinary(ccsdsPacket);
            uplinkLink.sendCommand(pc);

            LOG.debug("Sent CFDP PDU: APID={}, seq={}, pduLen={}", fileApid, sequenceCount - 1, pdu.length);

        } catch (Exception e) {
            LOG.error("Failed to send CFDP PDU", e);
        }
    }

    @Override
    public void streamClosed(Stream stream) {
        LOG.info("Stream {} closed", stream.getName());
    }

    /**
     * Build a CCSDS space packet with the given PDU as payload.
     * Packet structure:
     * - CCSDS Primary Header (6 bytes)
     * - F´ ComPacket Descriptor (2 bytes) = 0x0003 for file packets
     * - PDU payload
     */
    private byte[] buildSpacePacket(byte[] pdu, int seqCount) {
        // Total packet data = descriptor + PDU
        int totalDataLength = FW_PACKET_DESCRIPTOR_SIZE + pdu.length;
        int dataLenField = totalDataLength - 1;  // CCSDS convention: length - 1

        ByteBuffer bb = ByteBuffer.allocate(CCSDS_PRIMARY_HEADER_SIZE + totalDataLength);

        // Word 0: 3b version(0) | 1b type(1=TC) | 1b secHdr(0) | 11b APID
        int packetId = (0 << 13) | (1 << 12) | (0 << 11) | (fileApid & 0x07FF);
        bb.putShort((short) packetId);

        // Word 1: 2b seqFlags (0b11=standalone) | 14b seqCount
        int seqCtrl = (0b11 << 14) | (seqCount & 0x3FFF);
        bb.putShort((short) seqCtrl);

        // Word 2: 16b data length
        bb.putShort((short) dataLenField);

        // F´ ComPacket descriptor (0x0003 = FW_PACKET_FILE)
        bb.putShort((short) FW_PACKET_FILE_DESCRIPTOR);

        // CFDP PDU payload
        bb.put(pdu);

        return bb.array();
    }
}
