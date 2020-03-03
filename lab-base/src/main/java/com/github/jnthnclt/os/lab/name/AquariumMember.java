package com.github.jnthnclt.os.lab.name;

import com.google.common.primitives.UnsignedBytes;

import java.util.Arrays;

/**
 * @author jonathan.colt
 */
public class AquariumMember implements Comparable<AquariumMember> {

    private final byte[] member;

    public AquariumMember(byte[] member) {
        this.member = member;
    }

    public byte[] getMember() {
        return member;
    }

    @Override
    public int compareTo(AquariumMember o) {

        return UnsignedBytes.lexicographicalComparator().compare(member, o.member);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Arrays.hashCode(this.member);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        final AquariumMember other = (AquariumMember) obj;
        return Arrays.equals(this.member, other.member);
    }

    @Override
    public String toString() {
        return "AquariumMember{" + "member=" + Arrays.toString(member) + '}';
    }

}
