package com.neo.datamanager;

public class LoginStationSucc {

    public static final String SEP = "\001";
    private String timestamp;
    private String userName;
    private String computerName;
    private String ipAddress;
    private String status = "1";

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
        sb.append(timestamp).append(SEP);
        sb.append(userName).append(SEP);
        sb.append(computerName).append(SEP);
        sb.append(ipAddress).append(SEP);
        sb.append(status);
        return sb.toString();
    }
}
