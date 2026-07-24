package com.example.myproject;

import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.tctm.AbstractPacketPreprocessor;

/**
 * CFDP Packet Preprocessor - Extracts CFDP PDUs from CCSDS packets.
 * <p>
 * This preprocessor is currently not used. Instead, we use CfdpBridgeService
 * to reformat packets from tm_realtime into the cfdp_in stream with the
 * correct tuple structure expected by CfdpService.
 * <p>
 * Kept for reference and potential future use.
 */
public class CfdpPacketPreprocessor extends AbstractPacketPreprocessor {

    private static final int CCSDS_PRIMARY_HEADER_SIZE = 6;
    private static final int FILE_APID = 3;

    private int fileApid;

    public CfdpPacketPreprocessor(String yamcsInstance) {
        this(yamcsInstance, YConfiguration.emptyConfig());
    }

    public CfdpPacketPreprocessor(String yamcsInstance, YConfiguration config) {
        super(yamcsInstance, config);
        this.fileApid = config.getInt("fileApid", FILE_APID);
    }

    @Override
    public TmPacket process(TmPacket packet) {
        // Not currently used - see CfdpBridgeService
        return null;
    }
}
