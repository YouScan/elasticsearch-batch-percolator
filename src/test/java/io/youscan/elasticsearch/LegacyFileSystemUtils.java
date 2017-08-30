package io.youscan.elasticsearch;

import java.io.File;

public class LegacyFileSystemUtils {

    /**
     * Deletes the given files recursively. if <tt>deleteRoots</tt> is set to <code>true</code>
     * the given root files will be deleted as well. Otherwise only their content is deleted.
     */
    public static boolean deleteRecursively(File[] roots, boolean deleteRoots) {

        boolean deleted = true;
        for (File root : roots) {
            deleted &= deleteRecursively(root, deleteRoots);
        }
        return deleted;
    }

    /**
     * Deletes the given files recursively including the given roots.
     */
    public static boolean deleteRecursively(File... roots) {
        return deleteRecursively(roots, true);
    }

    /**
     * Delete the supplied {@link java.io.File} - for directories,
     * recursively delete any nested directories or files as well.
     *
     * @param root       the root <code>File</code> to delete
     * @param deleteRoot whether or not to delete the root itself or just the content of the root.
     * @return <code>true</code> if the <code>File</code> was deleted,
     *         otherwise <code>false</code>
     */
    public static boolean deleteRecursively(File root, boolean deleteRoot) {
        if (root != null) {
            File[] children = root.listFiles();
            if (children != null) {
                for (File aChildren : children) {
                    deleteRecursively(aChildren, true);
                }
            }

            if (deleteRoot) {
                return root.delete();
            } else {
                return true;
            }
        }
        return false;
    }
}
