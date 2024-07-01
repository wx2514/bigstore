package com.wuqing.business.bigstore.util.filterjudge;

public class FilterLikeJudge implements FilterJudge {

    private String search;

    private boolean firstLike;

    private boolean endLike;

    public FilterLikeJudge(String search) {
        this.search = search;
        this.firstLike = this.search.startsWith("*");
        if (this.firstLike) {
            this.search = this.search.substring(1);
        }
        this.endLike = this.search.endsWith("*");
        if (this.endLike) {
            this.search = this.search.substring(0, this.search.length() - 1);
        }
    }

    @Override
    public boolean isMath(String data) {
        if (data == null) {
            return false;
        }
        if (!this.firstLike && !this.endLike) {
            return data.equals(this.search);
        }
        if (!this.firstLike) {
            return data.startsWith(this.search);
        }
        if (!this.endLike) {
            return data.endsWith(this.search);
        }
        return false;
    }
}
