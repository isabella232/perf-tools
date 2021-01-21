package io.delay.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * The {@link DelayTransform} plugin adds a delay time (in milliseconds) while processing each input record.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Delay")
@Description("Adds a delay time (in milliseconds) while processing each input record.")
public class DelayTransform extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(DelayTransform.class);

  private final DelayConfig delayConfig;

  public DelayTransform(DelayConfig delayConfig) {
    this.delayConfig = delayConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();

    // set the schema for the output stage
    stageConfigurer.setOutputSchema(inputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    
    LOG.debug("Delay time set to {}ms", delayConfig.delayTime);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter)
      throws Exception {

    StructuredRecord.Builder builder = StructuredRecord.builder(input.getSchema());
    
    for (Schema.Field field: input.getSchema().getFields()) {
      String name = field.getName();
      if (input.get(name) != null) {
        builder.set(name, input.get(name));
      }
    }

    // sleep for the time specified by the user
    TimeUnit.MILLISECONDS.sleep(delayConfig.delayTime);

    emitter.emit(builder.build());
  }
  
  /**
   * Configurations for {@link DelayTransform}.
   */
  public static class DelayConfig extends PluginConfig {
    @Name("delayTime")
    @Description("The delay time to add while processing each record.")
    private final long delayTime;

    public DelayConfig(Long delayTime) {
      this.delayTime = delayTime;
    }
  }
}
