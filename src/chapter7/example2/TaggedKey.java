package chapter7.example2;

public class TaggedKey {
    private String carrierCode;

    private Integer tag;

    public TaggedKey() {
    }

    public TaggedKey(String carrierCode, Integer tag) {
        this.carrierCode = carrierCode;
        this.tag = tag;
    }

    public String getCarrierCode() {
        return carrierCode;
    }

    public Integer getTag() {
        return tag;
    }

    @Override
    pubilc int compareTo(TaggedKey key) {
        int result = this.carrierCode.compareTo(key.carrierCode);

        if (result == 0) {
            return this.tag.compareTo(key.tag);
        }
        return result;
    }



}
