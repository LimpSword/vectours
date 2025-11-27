package fr.alexandredch.vectours.data;

public record SearchParameters(double[] searchedVector, boolean allowIVF, boolean usePQ, boolean useHNSW, int topK) {

    public static class Builder {
        private double[] searchedVector;
        private boolean allowIVF = true;
        private boolean usePQ = false;
        private boolean useHNSW = false;
        private int topK = 10;

        public Builder searchedVector(double[] searchedVector) {
            this.searchedVector = searchedVector;
            return this;
        }

        public Builder allowIVF(boolean allowIVF) {
            this.allowIVF = allowIVF;
            return this;
        }

        public Builder usePQ(boolean usePQ) {
            this.usePQ = usePQ;
            return this;
        }

        public Builder useHNSW(boolean useHNSW) {
            this.useHNSW = useHNSW;
            return this;
        }

        public Builder topK(int topK) {
            this.topK = topK;
            return this;
        }

        public SearchParameters build() {
            return new SearchParameters(searchedVector, allowIVF, usePQ, useHNSW, topK);
        }
    }
}
