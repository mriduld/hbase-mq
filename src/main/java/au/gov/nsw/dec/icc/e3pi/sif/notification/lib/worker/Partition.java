package au.gov.nsw.dec.icc.e3pi.sif.notification.lib.worker;

import au.gov.nsw.dec.icc.e3pi.sif.notification.interfaces.IPartition;

import java.util.Objects;

class Partition implements IPartition{
    private final String id;

    Partition(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Partition that = (Partition) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("Partition [%s]", id);
    }
}
