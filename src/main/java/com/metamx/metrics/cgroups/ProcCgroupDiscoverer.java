package com.metamx.metrics.cgroups;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import com.metamx.metrics.CgroupUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class ProcCgroupDiscoverer implements CgroupDiscoverer
{
  private static final Logger LOG = new Logger(ProcCgroupDiscoverer.class);
  // TODO: discover `/proc` via whatever is mounted by `/proc`
  private static final String CGROUP_TYPE = "cgroup";
  private static final String PROC_TYPE = "proc";

  @Override
  public Path discover(final String cgroup, long pid)
  {
    Preconditions.checkNotNull(cgroup, "cgroup");
    // Wish List: find a way to cache these
    final File proc = proc();
    if (proc == null) {
      return null;
    }
    final File procMounts = getProcMounts(proc);
    final File procCgroups = getProcCgroups(proc);
    final File pidCgroups = getPidCgroups(proc, pid);
    final ProcCgroupsEntry procCgroupsEntry = getCgroupEntry(procCgroups, cgroup);
    final ProcMountsEntry procMountsEntry = getMountEntry(procMounts, cgroup);
    final ProcPidCgroupEntry procPidCgroupEntry = getPidCgroupEntry(pidCgroups, procCgroupsEntry.hierarchy);
    final File cgroupDir = new File(
        procMountsEntry.path.toFile(),
        procPidCgroupEntry.path
    );
    if (cgroupDir.exists() && cgroupDir.isDirectory()) {
      return cgroupDir.toPath();
    }
    LOG.warn("Invalid cgroup directory [%s]", cgroupDir);
    return null;
  }

  @VisibleForTesting
  public long getProbabyPid()
  {
    try {
      return CgroupUtil.getProbablyPID();
    }
    catch (CgroupUtil.IndeterminatePid indeterminatePid) {
      throw Throwables.propagate(indeterminatePid);
    }
  }

  @VisibleForTesting
  public File proc()
  {
    final File proc = Paths.get("/proc").toFile();
    Path foundProc = null;
    if (proc.exists() && proc.isDirectory()) {
      // Sanity check
      try {
        for (final String line : Files.readLines(new File(proc, "mounts"), Charsets.UTF_8)) {
          final ProcMountsEntry entry = ProcMountsEntry.parse(line);
          if (PROC_TYPE.equals(entry.type)) {
            if (proc.toPath().equals(entry.path)) {
              return proc;
            } else {
              foundProc = entry.path;
            }
          }
        }
      }
      catch (IOException e) {
        // Unlikely
        throw Throwables.propagate(e);
      }
      if (foundProc != null) {
        LOG.warn("Expected proc to be mounted on /proc, but was on [%s]", foundProc);
      } else {
        LOG.warn("No proc entry found in /proc/mounts");
      }
    } else {
      LOG.warn("/proc is not a valid directory");
    }
    return null;
  }

  @VisibleForTesting
  File getProcMounts(File proc)
  {
    return new File(proc, "mounts");
  }

  @VisibleForTesting
  File getProcCgroups(File proc)
  {
    return new File(proc, "cgroups");
  }

  @VisibleForTesting
  File getPidCgroups(File proc, long pid)
  {
    return new File(new File(proc, Long.toString(pid)), "cgroup");
  }

  final ProcPidCgroupEntry getPidCgroupEntry(final File pidCgroups, final int hierarchy)
  {
    final List<String> lines;
    try {
      lines = Files.readLines(pidCgroups, Charsets.UTF_8);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    for (final String line : lines) {
      final ProcPidCgroupEntry entry = ProcPidCgroupEntry.parse(line);
      if (hierarchy == entry.hierarchy) {
        return entry;
      }
    }
    throw new RuntimeException(StringUtils.safeFormat("No hierarchy found for [%d]", hierarchy));
  }

  final ProcCgroupsEntry getCgroupEntry(final File procCgroups, final String cgroup)
  {
    final List<String> lines;
    try {
      lines = Files.readLines(procCgroups, Charsets.UTF_8);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    for (final String line : lines) {
      if (line.startsWith("#")) {
        continue;
      }
      final ProcCgroupsEntry entry = ProcCgroupsEntry.parse(line);
      if (entry.enabled && cgroup.equals(entry.subsystem_name)) {
        return entry;
      }
    }
    throw new RuntimeException(StringUtils.safeFormat("Hierarchy for [%s] not found", cgroup));
  }

  final ProcMountsEntry getMountEntry(final File procMounts, final String cgroup)
  {
    final List<String> lines;
    try {
      lines = Files.readLines(procMounts, Charsets.UTF_8);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    for (final String line : lines) {
      final ProcMountsEntry entry = ProcMountsEntry.parse(line);
      if (CGROUP_TYPE.equals(entry.type) && entry.options.contains(cgroup)) {
        return entry;
      }
    }
    throw new RuntimeException(StringUtils.safeFormat("Cgroup [%s] not found", cgroup));
  }

  /**
   * Doesn't use the last two mount entries for priority/boot stuff
   */
  static class ProcMountsEntry
  {
    // Example: cgroup /sys/fs/cgroup/cpu,cpuacct cgroup rw,nosuid,nodev,noexec,relatime,cpu,cpuacct 0 0
    static ProcMountsEntry parse(String entry)
    {
      final String[] splits = entry.split(CgroupUtil.SPACE_MATCH, 6);
      Preconditions.checkArgument(splits.length == 6, "Invalid entry: [%s]", entry);
      return new ProcMountsEntry(
          splits[0],
          Paths.get(splits[1]),
          splits[2],
          ImmutableSet.copyOf(splits[3].split(CgroupUtil.COMMA_MATCH))
      );
    }

    final String dev;
    final Path path;
    final String type;
    final Set<String> options;

    ProcMountsEntry(String dev, Path path, String type, Collection<String> options)
    {
      this.dev = dev;
      this.path = path;
      this.type = type;
      this.options = ImmutableSet.copyOf(options);
    }
  }

  static class ProcCgroupsEntry
  {
    // Example Header: #subsys_name	hierarchy	num_cgroups	enabled
    static ProcCgroupsEntry parse(String entry)
    {
      final String[] splits = entry.split(Pattern.quote("\t"));
      return new ProcCgroupsEntry(
          splits[0],
          Integer.parseInt(splits[1]),
          Integer.parseInt(splits[2]),
          Integer.parseInt(splits[3]) == 1
      );
    }

    final String subsystem_name;
    final int hierarchy;
    final int num_cgroups;
    final boolean enabled;

    ProcCgroupsEntry(String subsystem_name, int hierarchy, int num_cgroups, boolean enabled)
    {
      this.subsystem_name = subsystem_name;
      this.hierarchy = hierarchy;
      this.num_cgroups = num_cgroups;
      this.enabled = enabled;
    }
  }

  static class ProcPidCgroupEntry
  {
    // example: 3:cpu,cpuacct:/system.slice/mesos-agent-spark.service/673550f3-69b9-4ef0-910d-762c8aaeda1c
    static ProcPidCgroupEntry parse(String entry)
    {
      final String[] splits = entry.split(CgroupUtil.COLON_MATCH, 3);
      Preconditions.checkArgument(splits.length == 3, "Invalid entry [%s]", entry);
      return new ProcPidCgroupEntry(Integer.parseInt(splits[0]), splits[1], splits[2]);
    }

    private final int hierarchy;
    private final String entrypoint;
    private final String path;

    ProcPidCgroupEntry(int hierarchy, String entrypoint, String path)
    {
      this.hierarchy = hierarchy;
      this.entrypoint = entrypoint;
      this.path = path;
    }
  }
}
