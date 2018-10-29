package org.onap.music.mdbc;

public class LockId {
    private String primaryKey;
    private String domain;
    private String lockReference;

    public LockId(String primaryKey, String domain, String lockReference){
        this.primaryKey = primaryKey;
        this.domain = domain;
        if(lockReference == null) {
            this.lockReference = "";
        }
        else{
            this.lockReference = lockReference;
        }
    }

    public String getFullyQualifiedLockKey(){
        return this.domain+"."+this.primaryKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getLockReference() {
        return lockReference;
    }

    public void setLockReference(String lockReference) {
        this.lockReference = lockReference;
    }
}
