/*
 * Copyright 2012 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.metamx.metrics;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.hyperic.sigar.Cpu;
import org.hyperic.sigar.DirUsage;
import org.hyperic.sigar.DiskUsage;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarFileNotFoundException;
import org.hyperic.sigar.SigarLoader;
import org.hyperic.sigar.Swap;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SysMonitor extends AbstractMonitor
{
  private static final Logger log = new Logger(SysMonitor.class);

  private final Sigar sigar = new Sigar();

  private final List<String> fsTypeWhitelist = ImmutableList.of("local");
  private final List<String> netAddressBlacklist = ImmutableList.of("0.0.0.0", "127.0.0.1");

  private final List<Stats> statsList;

  public SysMonitor()
  {
    this.statsList = new ArrayList<Stats>();
    this.statsList.addAll(
        Arrays.asList(
            new MemStats(),
            new FsStats(),
            new DiskStats(),
            new NetStats(),
            new CpuStats(),
            new SwapStats()
        )
    );
  }

  static {
    SigarLoader loader = new SigarLoader(Sigar.class);
    try {
      String libName = loader.getLibraryName();

      URL url = SysMonitor.class.getResource("/" + libName);
      if (url != null) {
        File tmpDir = File.createTempFile("yay", "yay");
        tmpDir.delete();
        tmpDir.mkdir();
        File nativeLibTmpFile = new File(tmpDir, libName);
        nativeLibTmpFile.deleteOnExit();
        StreamUtils.copyToFileAndClose(url.openStream(), nativeLibTmpFile);
        log.info("Loading sigar native lib at tmpPath[%s]", nativeLibTmpFile);
        loader.load(nativeLibTmpFile.getParent());
      } else {
        log.info("No native libs found in jar, letting the normal load mechanisms figger it out.");
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void addDirectoriesToMonitor(String[] dirList)
  {
    for (int i = 0; i < dirList.length; i++) {
      dirList[i] = dirList[i].trim();
    }
    statsList.add(new DirStats(dirList));
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Stats stats : statsList) {
      stats.emit(emitter);
    }
    return true;
  }

  private interface Stats
  {
    public void emit(ServiceEmitter emitter);
  }

  private class MemStats implements Stats
  {
    @Override
    public void emit(ServiceEmitter emitter)
    {
      Mem mem = null;
      try {
        mem = sigar.getMem();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Mem");
      }
      if (mem != null) {
        final Map<String, Long> stats = ImmutableMap.of(
            "sys/mem/max", mem.getTotal(),
            "sys/mem/used", mem.getUsed()
        );
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
    }
  }

  /**
   * Gets the swap stats from sigar and emits the periodic pages in & pages out of memory
   * along with the max swap and free swap memory.
   */
  private class SwapStats implements Stats
  {
    private long prevPageIn = 0, prevPageOut = 0;

    private SwapStats()
    {
      try {
        Swap swap = sigar.getSwap();
        this.prevPageIn = swap.getPageIn();
        this.prevPageOut = swap.getPageOut();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Swap");
      }
    }

    @Override
    public void emit(ServiceEmitter emitter)
    {
      Swap swap = null;
      try {
        swap = sigar.getSwap();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Swap");
      }
      if (swap != null) {
        long currPageIn = swap.getPageIn();
        long currPageOut = swap.getPageOut();

        final Map<String, Long> stats = ImmutableMap.of(
            "sys/swap/pageIn", (currPageIn - prevPageIn),
            "sys/swap/pageOut", (currPageOut - prevPageOut),
            "sys/swap/max", swap.getTotal(),
            "sys/swap/free", swap.getFree()
        );

        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }

        this.prevPageIn = currPageIn;
        this.prevPageOut = currPageOut;
      }
    }
  }

  /**
   * Gets the disk usage of a particular directory.
   */
  private class DirStats implements Stats
  {
    private final String[] dirList;

    private DirStats(String[] dirList)
    {
      this.dirList = dirList;
    }

    @Override
    public void emit(ServiceEmitter emitter)
    {
      for (String dir : dirList) {
        DirUsage du = null;
        try {
          du = sigar.getDirUsage(dir);
        }
        catch (SigarException e) {
          log.error("Failed to get DiskUsage for [%s] due to   [%s]", dir, e.getMessage());
        }
        if (du != null) {
          final Map<String, Long> stats = ImmutableMap.of(
              "sys/storage/used", du.getDiskUsage()
          );
          final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
              .setUser2(dir); // user2 because FsStats uses user2
          for (Map.Entry<String, Long> entry : stats.entrySet()) {
            emitter.emit(builder.build(entry.getKey(), entry.getValue()));
          }
        }
      }
    }
  }

  private class FsStats implements Stats
  {
    @Override
    public void emit(ServiceEmitter emitter)
    {
      FileSystem[] fss = null;
      try {
        fss = sigar.getFileSystemList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get FileSystem list");
      }
      if (fss != null) {
        log.debug("Found FileSystem list: [%s]", Joiner.on(", ").join(fss));
        for (FileSystem fs : fss) {
          final String name = fs.getDirName(); // (fs.getDevName() does something wonky here!)
          if (fsTypeWhitelist.contains(fs.getTypeName())) {
            FileSystemUsage fsu = null;
            try {
              fsu = sigar.getFileSystemUsage(name);
            }
            catch (SigarException e) {
              log.error(e, "Failed to get FileSystemUsage[%s]", name);
            }
            if (fsu != null) {
              final Map<String, Long> stats = ImmutableMap.of(
                  "sys/fs/max", fsu.getTotal() * 1024,
                  "sys/fs/used", fsu.getUsed() * 1024
              );
              final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
                  .setUser1(fs.getDevName())
                  .setUser2(fs.getDirName())
                  .setUser3(fs.getTypeName())
                  .setUser4(fs.getSysTypeName())
                  .setUser5(fs.getOptions().split(","));
              for (Map.Entry<String, Long> entry : stats.entrySet()) {
                emitter.emit(builder.build(entry.getKey(), entry.getValue()));
              }
            }
          } else {
            log.debug("Not monitoring fs stats for name[%s] with typeName[%s]", name, fs.getTypeName());
          }
        }
      }
    }
  }

  private class DiskStats implements Stats
  {
    private final KeyedDiff diff = new KeyedDiff();

    @Override
    public void emit(ServiceEmitter emitter)
    {
      FileSystem[] fss = null;
      try {
        fss = sigar.getFileSystemList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get FileSystem list");
      }
      if (fss != null) {
        log.debug("Found FileSystem list: [%s]", Joiner.on(", ").join(fss));
        for (FileSystem fs : fss) {
          final String name = fs.getDevName(); // (fs.getDirName() appears to give the same results here)
          if (fsTypeWhitelist.contains(fs.getTypeName())) {
            DiskUsage du = null;
            try {
              du = sigar.getDiskUsage(name);
            }
            catch (SigarException e) {
              log.error(e, "Failed to get DiskUsage[%s]", name);
            }
            if (du != null) {
              final Map<String, Long> stats = diff.to(
                  name, ImmutableMap.of(
                  "sys/disk/read/size", du.getReadBytes(),
                  "sys/disk/read/count", du.getReads(),
                  "sys/disk/write/size", du.getWriteBytes(),
                  "sys/disk/write/count", du.getWrites()
              )
              );
              if (stats != null) {
                final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
                    .setUser1(fs.getDevName())
                    .setUser2(fs.getDirName())
                    .setUser3(fs.getTypeName())
                    .setUser4(fs.getSysTypeName())
                    .setUser5(fs.getOptions().split(","));
                for (Map.Entry<String, Long> entry : stats.entrySet()) {
                  emitter.emit(builder.build(entry.getKey(), entry.getValue()));
                }
              }
            }
          } else {
            log.debug("Not monitoring disk stats for name[%s] with typeName[%s]", name, fs.getTypeName());
          }
        }
      }
    }
  }

  private class NetStats implements Stats
  {
    private final KeyedDiff diff = new KeyedDiff();

    @Override
    public void emit(ServiceEmitter emitter)
    {
      String[] ifaces = null;
      try {
        ifaces = sigar.getNetInterfaceList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get NetInterface list");
      }
      if (ifaces != null) {
        log.debug("Found NetInterface list: [%s]", Joiner.on(", ").join(ifaces));
        for (String name : ifaces) {
          NetInterfaceConfig netconf = null;
          try {
            netconf = sigar.getNetInterfaceConfig(name);
          }
          catch (SigarException e) {
            log.error(e, "Failed to get NetInterfaceConfig[%s]", name);
          }
          if (netconf != null) {
            if (!(netAddressBlacklist.contains(netconf.getAddress()))) {
              NetInterfaceStat netstat = null;
              try {
                netstat = sigar.getNetInterfaceStat(name);
              }
              catch (SigarException e) {
                log.error(e, "Failed to get NetInterfaceStat[%s]", name);
              }
              if (netstat != null) {
                final Map<String, Long> stats = diff.to(
                    name, ImmutableMap.of(
                    "sys/net/read/size", netstat.getRxBytes(),
                    "sys/net/write/size", netstat.getTxBytes()
                )
                );
                if (stats != null) {
                  final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
                      .setUser1(netconf.getName())
                      .setUser2(netconf.getAddress())
                      .setUser3(netconf.getHwaddr());
                  for (Map.Entry<String, Long> entry : stats.entrySet()) {
                    emitter.emit(builder.build(entry.getKey(), entry.getValue()));
                  }
                }
              }
            } else {
              log.debug("Not monitoring net stats for name[%s] with address[%s]", name, netconf.getAddress());
            }
          }
        }
      }
    }
  }

  private class CpuStats implements Stats
  {
    private final KeyedDiff diff = new KeyedDiff();

    @Override
    public void emit(ServiceEmitter emitter)
    {
      Cpu[] cpus = null;
      try {
        cpus = sigar.getCpuList();
      }
      catch (SigarException e) {
        log.error(e, "Failed to get Cpu list");
      }
      if (cpus != null) {
        log.debug("Found Cpu list: [%s]", Joiner.on(", ").join(cpus));
        for (int i = 0; i < cpus.length; ++i) {
          final Cpu cpu = cpus[i];
          final String name = Integer.toString(i);
          final Map<String, Long> stats = diff.to(
              name, ImmutableMap.of(
              "user", cpu.getUser(), // user = Δuser / Δtotal
              "sys", cpu.getSys(),  // sys  = Δsys  / Δtotal
              "nice", cpu.getNice(), // nice = Δnice / Δtotal
              "wait", cpu.getWait(), // wait = Δwait / Δtotal
              "_total", cpu.getTotal() // (not reported)
          )
          );
          if (stats != null) {
            final long total = stats.remove("_total");
            for (Map.Entry<String, Long> entry : stats.entrySet()) {
              final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
                  .setUser1(name)
                  .setUser2(entry.getKey());
              emitter.emit(builder.build("sys/cpu", entry.getValue() * 100 / total)); // [0,100]
            }
          }
        }
      }
    }
  }
}
