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

import com.google.common.base.Throwables;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.hyperic.jni.ArchLoaderException;
import org.hyperic.jni.ArchNotSupportedException;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarLoader;

public class SigarUtil
{
  private static final Logger log = new Logger(SigarUtil.class);

  // Note: this is required to load the sigar native lib.
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
    catch (ArchNotSupportedException | ArchLoaderException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * SigarHolder class is initialized after SigarUtil, that guarantees that SIGAR = new Sigar() is executed after static
   * block (which loads the library) of SigarUtil is executed. This is anyway guaranteed by JLS if SIGAR static field
   * goes below the static block in textual order, but fragile e. g. if someone applies automatic reformatting and the
   * static field is moved above the static block.
   */
  private static class SigarHolder
  {
    private static final Sigar SIGAR = new Sigar();
  }

  public static Sigar getSigar()
  {
    return SigarHolder.SIGAR;
  }

}
