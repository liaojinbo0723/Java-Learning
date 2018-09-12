package com.neo.datamanager;

public class LoginStationFail {

    public static final String SEP = "\001";
    private String logId;
    private String timestamp;
    private String userName;
    private String computerName;
    private String ipAddress;
    private String status = "0";

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getComputerName() {
        return computerName;
    }

    public void setComputerName(String computerName) {
        this.computerName = computerName;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(logId).append(SEP);
        sb.append(timestamp).append(SEP);
        sb.append(userName).append(SEP);
        sb.append(computerName).append(SEP);
        sb.append(ipAddress).append(SEP);
        sb.append(status);
        return sb.toString();
    }
}
