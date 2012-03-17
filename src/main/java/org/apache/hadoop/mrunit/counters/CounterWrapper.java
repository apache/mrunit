package org.apache.hadoop.mrunit.counters;


/**
 * Wrapper around Counters from both mapred and mapreduce packages so that we
 * can work with both counter classes in the same way.
 */
public class CounterWrapper {

  /**
   * Wrap old mapred counter class
   */
  private org.apache.hadoop.mapred.Counters mapred;

  /**
   * Wrap new mapreduce counter class
   */
  private org.apache.hadoop.mapreduce.Counters mapreduce;

  /**
   * Wrap old counter object
   * @param counters
   */
  public CounterWrapper(org.apache.hadoop.mapred.Counters counters) {
    mapred = counters;
  }

  /**
   * Wrap new counter object
   * @param counters
   */
  public CounterWrapper(org.apache.hadoop.mapreduce.Counters counters) {
    mapreduce = counters;
  }

  /**
   * Get counter value based on Enumeration
   * @param e
   * @return
   */
  public long findCounterValue(Enum e) {
    if(mapred != null) {
      return mapred.findCounter(e).getValue();
    } else {
      return mapreduce.findCounter(e).getValue();
    }
  }

  /**
   * Get counter value based on name
   * @param group
   * @param name
   * @return
   */
  public long findCounterValue(String group, String name) {
    if(mapred != null) {
      return mapred.findCounter(group, name).getValue();
    } else {
      return mapreduce.findCounter(group, name).getValue();
    }
  }
}
