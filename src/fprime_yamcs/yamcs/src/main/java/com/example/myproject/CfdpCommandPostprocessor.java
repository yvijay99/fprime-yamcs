package com.example.myproject;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.yamcs.YConfiguration;
import org.yamcs.cmdhistory.CommandHistoryPublisher;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.tctm.CommandPostprocessor;

/**
 * CFDP Command Postprocessor - Wraps CFDP PDUs with CCSDS headers.
 * <p>
 * This postprocessor receives raw CFDP PDUs from the CFDP service,
 * adds CCSDS primary headers, and forwards them to the data link.
 * <p>
 * Configuration in yamcs.myproject.yaml:
 *
 * <pre>
 * dataLinks:
 *   - name: UDP_TC_OUT
 *     virtualChannels:
 *       - vcId: 1
 *         commandPostprocessorClassName: com.example.myproject.CfdpCommandPostprocessor
 *         stream: "cfdp_out"
 * </pre>
 */
public class CfdpCommandPostprocessor implements CommandPostprocessor {

    private static final int CCSDS_PRIMARY_HEADER_SIZE = 6;
    private static final int FILE_APID = 3; // Default F' APID for file transfers

    private int fileApid;
    private AtomicInteger sequenceCount = new AtomicInteger(0);
    private CommandHistoryPublisher commandHistory;

    // Constructor without YAML configuration
    public CfdpCommandPostprocessor(String yamcsInstance) {
        this(yamcsInstance, YConfiguration.emptyConfig());
    }

    // Constructor with YAML configuration
    public CfdpCommandPostprocessor(String yamcsInstance, YConfiguration config) {
        // Allow APID to be configured, default to 3
        this.fileApid = config.getInt("fileApid", FILE_APID);
    }

    @Override
    public void setCommandHistoryPublisher(CommandHistoryPublisher commandHistory) {
        this.commandHistory = commandHistory;
    }

    @Override
    public byte[] process(PreparedCommand pc) {
        byte[] pdu = pc.getBinary();

        // Build CCSDS primary header (6 bytes)
        ByteBuffer header = ByteBuffer.allocate(CCSDS_PRIMARY_HEADER_SIZE);

        // Byte 0-1: Version(3b) | Type(1b) | SecHdr(1b) | APID(11b)
        // Version = 0 (3 bits)
        // Type = 1 (telecommand, 1 bit)
        // SecHdr = 0 (no secondary header, 1 bit)
        // APID = fileApid (11 bits)
        int byte0_1 = (0 << 13) | (1 << 11) | (0 << 11) | (fileApid & 0x7FF);
        header.putShort((short) byte0_1);

        // Byte 2-3: Seq Flags(2b) | Seq Count(14b)
        // Seq Flags = 3 (unsegmented, standalone packet)
        // Seq Count = incrementing counter (14 bits)
        int seqCount = sequenceCount.getAndIncrement() & 0x3FFF;
        int byte2_3 = (3 << 14) | seqCount;
        header.putShort((short) byte2_3);

        // Byte 4-5: Packet Data Length (16 bits)
        // This is the length of the data field MINUS 1 (per CCSDS spec)
        int packetDataLength = pdu.length - 1;
        header.putShort((short) packetDataLength);

        // Combine CCSDS header + CFDP PDU
        byte[] packet = new byte[CCSDS_PRIMARY_HEADER_SIZE + pdu.length];
        System.arraycopy(header.array(), 0, packet, 0, CCSDS_PRIMARY_HEADER_SIZE);
        System.arraycopy(pdu, 0, packet, CCSDS_PRIMARY_HEADER_SIZE, pdu.length);

        // Publish sequence count to command history
        if (commandHistory != null) {
            commandHistory.publish(pc.getCommandId(), "ccsds-seqcount", seqCount);
            commandHistory.publish(pc.getCommandId(), PreparedCommand.CNAME_BINARY, packet);
        }

        return packet;
    }
}
