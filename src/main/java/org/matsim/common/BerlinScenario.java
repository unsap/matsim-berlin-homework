package org.matsim.common;

public enum BerlinScenario {

    BASE(0, "BASE"),
    GR_HS(1, "GR-HS"),
    GR_WS(2, "GR-WS"),
    KR_HS(3, "KR-HS"),
    KB(4, "KB"),
    MV(5, "MV"),
    S1(6, "S1"),
    S2(7, "S2"),
    S3(8, "S3");

    private final int scenarioNumber;
    private final String scenarioName;

    BerlinScenario(int scenarioNumber, String scenarioName) {
        this.scenarioNumber = scenarioNumber;
        this.scenarioName = scenarioName;
    }

    public String getDirectoryName() {
        return String.format("%d.%s", scenarioNumber, scenarioName);
    }

    public String getFilePrefix() {
        return scenarioName;
    }

}
