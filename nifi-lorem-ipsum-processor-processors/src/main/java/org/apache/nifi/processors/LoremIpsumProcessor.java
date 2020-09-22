package org.apache.nifi.processors;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"lorem-ipsum", "source", "input"})
@WritesAttributes({
    @WritesAttribute(attribute = "number-of-lines", description = "How many lines are generated.")
})
@CapabilityDescription("Creates FlowFiles from lorem ipsum classic lines.")
public class LoremIpsumProcessor extends AbstractSessionFactoryProcessor {

  public static final PropertyDescriptor NUMBER_OF_LINES = new PropertyDescriptor.Builder()
      .name("Number of lines")
      .description("Number of lines, which should be generated")
      .required(true)
      .addValidator(StandardValidators.INTEGER_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.NONE)
      .build();

  private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Collections.singletonList(NUMBER_OF_LINES));

  public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All content routed to success").build();

  private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(Collections.singleton(REL_SUCCESS));

  private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return PROPERTY_DESCRIPTORS;
  }

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory)
      throws ProcessException {
        int numberOfLinesToGenerate = context.getProperty(NUMBER_OF_LINES).asInteger();

        String generatedText = IntStream
            .range(0, numberOfLinesToGenerate)
            .mapToObj((int index) -> LOREM_IPSUM)
            .collect(Collectors.joining("\n"));

    ProcessSession session = sessionFactory.createSession();
    FlowFile flowFile = session.create();
    flowFile = session.putAttribute(flowFile, NUMBER_OF_LINES.getName(), Integer.toString(numberOfLinesToGenerate));
    flowFile = session.write(flowFile, (outputStream) -> {
      outputStream.write(generatedText.getBytes(Charset.forName("UTF-8")));
    });
    session.transfer(flowFile, REL_SUCCESS);
    session.commit();
  }
}
