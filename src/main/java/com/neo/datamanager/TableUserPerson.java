package com.neo.datamanager;

public class TableUserPerson {
    private String id;
    private String username;
    private String userType;
    private String userNo;
    private String realName;
    private String gender;
    private String birthdate;
    private String email;
    private String mobile;
    private String idCardNo;
    private String idCardType;
    private String loginPwd;
    private String paymentPwd;
    private String status;
    private String securityLevel;
    private String elecSign;
    private String createTime;
    private String lstModTime;
    private String syncCreateTime;
    private String syncUpdateTime;
    private String digest;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUserType() {
        return userType;
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public String getUserNo() {
        return userNo;
    }

    public void setUserNo(String userNo) {
        this.userNo = userNo;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getBirthdate() {
        return birthdate;
    }

    public void setBirthdate(String birthdate) {
        this.birthdate = birthdate;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getIdCardNo() {
        return idCardNo;
    }

    public void setIdCardNo(String idCardNo) {
        this.idCardNo = idCardNo;
    }

    public String getIdCardType() {
        return idCardType;
    }

    public void setIdCardType(String idCardType) {
        this.idCardType = idCardType;
    }

    public String getLoginPwd() {
        return loginPwd;
    }

    public void setLoginPwd(String loginPwd) {
        this.loginPwd = loginPwd;
    }

    public String getPaymentPwd() {
        return paymentPwd;
    }

    public void setPaymentPwd(String paymentPwd) {
        this.paymentPwd = paymentPwd;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSecurityLevel() {
        return securityLevel;
    }

    public void setSecurityLevel(String securityLevel) {
        this.securityLevel = securityLevel;
    }

    public String getElecSign() {
        return elecSign;
    }

    public void setElecSign(String elecSign) {
        this.elecSign = elecSign;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getLstModTime() {
        return lstModTime;
    }

    public void setLstModTime(String lstModTime) {
        this.lstModTime = lstModTime;
    }

    public String getSyncCreateTime() {
        return syncCreateTime;
    }

    public void setSyncCreateTime(String syncCreateTime) {
        this.syncCreateTime = syncCreateTime;
    }

    public String getSyncUpdateTime() {
        return syncUpdateTime;
    }

    public void setSyncUpdateTime(String syncUpdateTime) {
        this.syncUpdateTime = syncUpdateTime;
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    public String getColumnValue(String column){
        String colResult = column;
        if(column.equals("")){
            colResult = "null";
        }else {
            colResult = "'" + column + "'";
        }
        return colResult;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("select ");
        sb.append(getColumnValue(id)).append(',');
        sb.append(getColumnValue(username)).append(',');
        sb.append(getColumnValue(userType)).append(',');
        sb.append(getColumnValue(userNo)).append(',');
        sb.append(getColumnValue(realName)).append(',');
        sb.append(getColumnValue(gender)).append(',');
        sb.append(getColumnValue(birthdate)).append(',');
        sb.append(getColumnValue(email)).append(',');
        sb.append(getColumnValue(mobile)).append(',');
        sb.append(getColumnValue(idCardNo)).append(',');
        sb.append(getColumnValue(idCardType)).append(',');
        sb.append(getColumnValue(loginPwd)).append(',');
        sb.append(getColumnValue(paymentPwd)).append(',');
        sb.append(getColumnValue(status)).append(',');
        sb.append(getColumnValue(securityLevel)).append(',');
        sb.append(getColumnValue(elecSign)).append(',');
        sb.append(getColumnValue(createTime)).append(',');
        sb.append(getColumnValue(lstModTime)).append(',');
        sb.append(getColumnValue(syncCreateTime)).append(',');
        sb.append(getColumnValue(syncUpdateTime)).append(',');
        sb.append(getColumnValue(digest)).append(";");
        return sb.toString();
    }
}
