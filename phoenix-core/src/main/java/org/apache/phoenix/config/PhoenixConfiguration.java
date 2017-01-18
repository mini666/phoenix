/**
 * 
 */
package org.apache.phoenix.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.util.VersionInfo;

/**
 * @author mini666
 *
 */
public class PhoenixConfiguration extends HBaseConfiguration {

  @SuppressWarnings("deprecation")
	private PhoenixConfiguration() {
  }

	public static Configuration addHbaseResources(Configuration conf) {
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");
    conf.addResource("phoenix-default.xml");
    conf.addResource("phoenix-site.xml");
    
    checkDefaultsVersion(conf);
    HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(conf);
    return conf;
  }
	

  /**
   * Creates a Configuration with HBase resources
   * @return a Configuration with HBase resources
   */
  public static Configuration create() {
    Configuration conf = new Configuration();
    // In case HBaseConfiguration is loaded from a different classloader than
    // Configuration, conf needs to be set with appropriate class loader to resolve
    // HBase resources.
    conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    return addHbaseResources(conf);
  }

  /**
   * @param that Configuration to clone.
   * @return a Configuration created with the hbase-*.xml files plus
   * the given configuration.
   */
  public static Configuration create(final Configuration that) {
    Configuration conf = create();
    merge(conf, that);
    return conf;
  }

	private static void checkDefaultsVersion(Configuration conf) {
    if (conf.getBoolean("hbase.defaults.for.version.skip", Boolean.FALSE)) return;
    String defaultsVersion = conf.get("hbase.defaults.for.version");
    String thisVersion = VersionInfo.getVersion();
    if (!thisVersion.equals(defaultsVersion)) {
      throw new RuntimeException(
        "hbase-default.xml file seems to be for an older version of HBase (" +
        defaultsVersion + "), this version is " + thisVersion);
    }
  }
	
}
