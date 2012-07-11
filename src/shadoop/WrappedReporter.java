package shadoop;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

public class WrappedReporter extends StatusReporter {

  private transient Counters counters = new Counters();

  @Override
  public Counter getCounter(Enum<?> name) {
    return counters == null ? null : counters.findCounter(name);
  }
  
  @Override
  public Counter getCounter(String group, String name) {
    Counters.Counter counter = null;
    if (counters != null) {
      counter = counters.findCounter(group, name);
    }
    return counter;    
  }

  @Override
  public void progress() {
  }

  @Override
  public void setStatus(String status) {
  }

}
