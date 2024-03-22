package io.siggi.anvilcompactor;

import io.siggi.anvilregionformat.AnvilCoordinate;
import io.siggi.anvilregionformat.AnvilRegion;
import io.siggi.anvilregionformat.ChunkCoordinate;
import io.siggi.anvilregionformat.ChunkData;
import io.siggi.nbt.NBTCompound;
import io.siggi.nbt.NBTList;
import io.siggi.nbt.NBTTool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class AnvilCompactor {
    public static void main(String[] args) throws Throwable {
        File world = new File(args[0]);
        compactWorld(world, true);
    }

    private static void compactWorld(File file, boolean avoidCopyingEmptyChunks) throws IOException {
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("Target is not a directory!");
        }
        File region = new File(file, "region");
        File entities = new File(file, "entities");
        File poi = new File(file, "poi");

        if (!region.isDirectory()) {
            throw new IllegalArgumentException("region directory does not exist, is this a Minecraft world?");
        }

        Set<ChunkCoordinate> chunkCoordinatesThatExist = compactRegion(region, avoidCopyingEmptyChunks);

        if (entities.isDirectory()) {
            compactMetadata(entities, chunkCoordinatesThatExist);
        }

        if (poi.isDirectory()) {
            compactMetadata(poi, chunkCoordinatesThatExist);
        }

        removePlayerData(new File(file, "level.dat"));
    }

    private static Set<ChunkCoordinate> compactRegion(File file, boolean avoidCopyingEmptyChunks) throws IOException {
        File bak = new File(file.getParentFile(), file.getName() + ".bak");
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("Target is not a directory!");
        }
        if (bak.exists()) {
            throw new IllegalArgumentException("Backup already exists!");
        }
        file.renameTo(bak);
        Set<ChunkCoordinate> copiedCoordinates = new HashSet<>();
        try (AnvilRegion from = AnvilRegion.open(bak);
             AnvilRegion to = AnvilRegion.open(file)) {
            copyChunks(
                    from, to,
                    null,
                    (chunk, data) -> {
                        NBTCompound compound = NBTTool.deserialize(data.getDecompressedData());
                        boolean shouldCopy = false;
                        checks:
                        if (!avoidCopyingEmptyChunks) {
                            shouldCopy = true;
                        } else if (has(compound, "Level")) {
                            // 1.8 - 1.17
                            NBTCompound level = compound.getCompound("Level");
                            if (has(level, "Sections")) {
                                NBTList sections = level.getList("Sections");
                                shouldCopy = sections.size() != 0;
                            } else {
                                shouldCopy = true;
                            }
                        } else if (has(compound, "sections")) {
                            // 1.18 - 1.20.4+
                            NBTList sections = compound.getList("sections");
                            for (int i = 0; i < sections.size(); i++) {
                                NBTCompound chunkSection = sections.getCompound(i);
                                NBTCompound blockStates = chunkSection.getCompound("block_states");
                                NBTList palette = blockStates.getList("palette");
                                if (palette.size() != 1) {
                                    shouldCopy = true;
                                    break checks;
                                }
                                NBTCompound firstItem = palette.getCompound(0);
                                if (!firstItem.getString("Name").equals("minecraft:air")) {
                                    shouldCopy = true;
                                    break checks;
                                }
                                long[] dataArray = blockStates.getLongArray("data");
                                if (dataArray != null && dataArray.length != 0) {
                                    shouldCopy = true;
                                    break checks;
                                }
                            }
                        } else {
                            shouldCopy = true;
                        }
                        if (shouldCopy) {
                            copiedCoordinates.add(chunk);
                        }
                        return shouldCopy;
                    }
            );
        }
        return copiedCoordinates;
    }

    private static void compactMetadata(File file, Set<ChunkCoordinate> chunks) throws IOException {
        File bak = new File(file.getParentFile(), file.getName() + ".bak");
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("Target is not a directory!");
        }
        if (bak.exists()) {
            throw new IllegalArgumentException("Backup already exists!");
        }
        file.renameTo(bak);
        try (AnvilRegion from = AnvilRegion.open(bak);
             AnvilRegion to = AnvilRegion.open(file)) {
            copyChunks(
                    from, to,
                    chunks::contains,
                    null
            );
        }
    }

    private static void copyChunks(AnvilRegion from, AnvilRegion to, Function<ChunkCoordinate, Boolean> shouldConsiderChunk, BiFunction<ChunkCoordinate, ChunkData, Boolean> shouldCopyChunk) throws IOException {
        List<AnvilCoordinate> regions = from.getRegions(new ArrayList<>());
        for (AnvilCoordinate coordinate : regions) {
            List<ChunkCoordinate> chunks = from.getChunks(coordinate, new ArrayList<>(), true);
            for (ChunkCoordinate chunk : chunks) {
                if (shouldConsiderChunk != null && !shouldConsiderChunk.apply(chunk)) continue;
                ChunkData data = from.read(chunk);
                if (data == null) continue;
                if (shouldCopyChunk != null && !shouldCopyChunk.apply(chunk, data)) continue;
                to.write(chunk, data);
            }
        }
    }

    private static boolean has(NBTCompound compound, String key) {
        return compound.keySet().contains(key);
    }

    private static void removePlayerData(File file) throws IOException {
        File bakFile = new File(file.getParentFile(), file.getName() + ".bak");
        if (bakFile.exists()) {
            throw new IllegalArgumentException("Backup already exists!");
        }
        NBTCompound compound;
        try (GZIPInputStream in = new GZIPInputStream(new FileInputStream(file))) {
            compound = NBTTool.deserialize(in);
        }
        if (compound.getCompound("Player").keySet().isEmpty()) return;
        file.renameTo(bakFile);
        compound.setCompound("Player", new NBTCompound());
        try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(file))) {
            NBTTool.serialize(out, compound);
        }
    }
}
