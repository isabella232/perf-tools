package io.delay.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import org.junit.Assert;
import org.junit.Test;

/** Test cases for Delay Transform. */
public class DelayTransformTest {

  private static final Schema INPUT =
      Schema.recordOf(
          "input",
          Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final long DELAY_TIME = 500;

  /**
   * Tests whether the time taken to pass one record through the transform method is greater than
   * the delay time set.
   *
   * @throws Exception
   */
  @Test
  public void testDelayTime() throws Exception {
    DelayTransform.DelayConfig config = new DelayTransform.DelayConfig(DELAY_TIME);
    Transform<StructuredRecord, StructuredRecord> transform = new DelayTransform(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    long transformStartTime = System.currentTimeMillis();
    transform.transform(
        StructuredRecord.builder(INPUT)
            .set("a", "1")
            .set("b", "2")
            .set("c", "3")
            .set("d", "4")
            .set("e", "5")
            .build(),
        emitter);
    long transformEndTime = System.currentTimeMillis();

    int totalTransformTime = (int) (transformEndTime - transformStartTime);

    // total 'transform' time for one record should be greater than the configured delay time.
    Assert.assertTrue(
        totalTransformTime + " should be greater than " + DELAY_TIME,
        totalTransformTime > DELAY_TIME);
  }
}
