package jamsesso.meshmap;

import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.UUID;

@EqualsAndHashCode
public class Key implements Serializable {
    private UUID hashing = UUID.randomUUID();

    public Key(UUID id){
        this.hashing = id;
    }

    public UUID getId(){
        return hashing;
    }

    /**
     * This method is used for compare if the key is between two specific keys
     * right included. This comparison is made in a circular way.
     *
     * @param key1 . The first key of the comparison
     * @param key2 . The second key of the comparison
     * @return Returns true if the key is between the keys that are received as
     * parameter
     */
    public boolean isBetweenRightIncluded(UUID key1, UUID key2) {
        if (key1.compareTo(key2) == -1) {
            if (hashing.compareTo(key1) == 1
                    && (hashing.compareTo(key2) == -1 || hashing
                    .compareTo(key2) == 0)) {
                return true;
            }
        } else {
            if (key1.compareTo(key2) == 1) {
                if ((hashing.compareTo(key1) == 1 && hashing
                        .compareTo(key2) == 1)
                        || hashing.compareTo(key2) == 0) {
                    return true;
                } else {
                    if (hashing.compareTo(key1) == -1
                            && hashing.compareTo(key2) == -1) {
                        return true;
                    }
                }
            } else {
                if (hashing.compareTo(key2) == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * This method is used for compare if the key is between two specific keys.
     * This comparison is made in a circular way
     *
     * @param key1 . The first key of the comparison
     * @param key2 . The second key of the comparison
     * @return Returns true if the key is between the keys that are received as
     * parameter
     */
    public boolean isBetween(UUID key1, UUID key2) {
        if (key1.compareTo(key2) == -1) {
            if (hashing.compareTo(key1) == 1
                    && (hashing.compareTo(key2) == -1)) {
                return true;
            }
        } else {
            if (key1.compareTo(key2) == 1) {
                if (hashing.compareTo(key1) == 1
                        && hashing.compareTo(key2) == 1) {
                    return true;
                } else {
                    if (hashing.compareTo(key1) == -1
                            && hashing.compareTo(key2) == -1) {
                        return true;
                    }
                }
            } else {
                if (hashing.compareTo(key2) == 0) {
                    return true;
                }
            }
        }

        return false;
    }

}
