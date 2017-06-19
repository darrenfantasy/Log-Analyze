package com.bqss.analyze;


import java.io.Serializable;

/**
 * Created by darrenfantasy on 2017/5/18.
 */
public class QiNiuResultItem implements Serializable{
    private Long mId;
    private String mIP;
    private Long mTime;
    private String mUrl;

    public Long getmId() {
        return mId;
    }

    public void setmId(Long mId) {
        this.mId = mId;
    }

    public String getmIP() {
        return mIP;
    }

    public void setmIP(String mIP) {
        this.mIP = mIP;
    }

    public Long getmTime() {
        return mTime;
    }

    public void setmTime(Long mTime) {
        this.mTime = mTime;
    }

    public String getmUrl() {
        return mUrl;
    }

    public void setmUrl(String mUrl) {
        this.mUrl = mUrl;
    }
}
