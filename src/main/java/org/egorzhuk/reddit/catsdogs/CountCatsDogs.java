package org.egorzhuk.reddit.catsdogs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.IOUtils;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.vision.v1.Vision;
import com.google.api.services.vision.v1.VisionScopes;
import com.google.api.services.vision.v1.model.*;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableList;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * The pipeline
 */
public class CountCatsDogs {

    public static final String REDDIT_IMGUR_CATS_DOGS = "bq-playground-1366:reddit.imgur_cats_dogs";

    public static void main(String[] args) {
        //Read cmd arguments and use custom options
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        //create pipeline
        final Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(BigQueryIO.Read.from(options.getInput()))
                .apply(new CountLabels())
                .apply(ParDo.of(new FormatAsTextFn()))
                .apply(TextIO.Write.named("Cats&Dogs").to(options.getOutput()));

        pipeline.run();
    }

    private static interface Options extends DataflowPipelineOptions {
        @Description("Table to read from, specified as "
                + "<project_id>:<dataset_id>.<table_id>")
        @Default.String(REDDIT_IMGUR_CATS_DOGS)
        String getInput();

        void setInput(String value);

        @Description("Text file to write to")
        @Default.InstanceFactory(OutputFactory.class)
        String getOutput();

        void setOutput(String value);

        /**
         * Copied from Google examples https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples
         * Returns "gs://${YOUR_STAGING_DIRECTORY}/counts.txt" as the default destination.
         */
        public static class OutputFactory implements DefaultValueFactory<String> {
            @Override
            public String create(PipelineOptions options) {
                DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
                if (dataflowOptions.getStagingLocation() != null) {
                    return GcsPath.fromUri(dataflowOptions.getStagingLocation())
                            .resolve("counts.txt").toString();
                } else {
                    throw new IllegalArgumentException("Must specify --output or --stagingLocation");
                }
            }
        }
    }

    /**
     * Copied from Google examples https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples
     * A DoFn that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
        @Override
        public void processElement(ProcessContext c) {
            c.output(c.element().getKey() + ": " + c.element().getValue());
        }
    }


    private static class GetUrls extends com.google.cloud.dataflow.sdk.transforms.DoFn<TableRow, URL> {
        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            final String res = processContext.element().getF().get(0).getV().toString();
            try {
                final URL url = new URL(res + ".png");
                processContext.output(url);
            } catch (MalformedURLException e) {
                //No output
            }
        }
    }

    private static class GetLabels extends com.google.cloud.dataflow.sdk.transforms.DoFn<URL, String> {


        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            final Vision vision = connect(processContext.getPipelineOptions().as(Options.class).getProject());
            final URL url = processContext.element();
            final byte[] imageBytes = getImageBytes(url);
            final List<EntityAnnotation> entityAnnotations = labelImage(vision, imageBytes);
            entityAnnotations.forEach((a) -> processContext.output(a.getDescription()));
        }

        private byte[] getImageBytes(URL url) throws IOException {
            byte[] image;
            try (InputStream inputStream = url.openStream(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                IOUtils.copy(inputStream, baos);
                image = baos.toByteArray();
            }
            return image;
        }

        private Vision connect(final String appName) throws IOException, GeneralSecurityException {
            GoogleCredential credential =
                    GoogleCredential.getApplicationDefault().createScoped(VisionScopes.all());
            JacksonFactory jsonFactory = JacksonFactory.getDefaultInstance();
            return new Vision.Builder(GoogleNetHttpTransport.newTrustedTransport(), jsonFactory, credential)
                    .setApplicationName(appName)
                    .build();
        }


        /**
         * Copied from Google examples https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/vision/label/
         */
        private List<EntityAnnotation> labelImage(final Vision vision, final byte[] imageBytes) throws IOException {

            AnnotateImageRequest request =
                    new AnnotateImageRequest()
                            .setImage(new Image().encodeContent(imageBytes))
                            .setFeatures(ImmutableList.of(
                                    new Feature()
                                            .setType("LABEL_DETECTION")
                                            .setMaxResults(5)));
            Vision.Images.Annotate annotate =
                    vision.images()
                            .annotate(new BatchAnnotateImagesRequest().setRequests(ImmutableList.of(request)));
            // Due to a bug: requests to Vision API containing large images fail when GZipped.
            annotate.setDisableGZipContent(true);
            // [END construct_request]

            // [START parse_response]
            BatchAnnotateImagesResponse batchResponse = annotate.execute();
            assert batchResponse.getResponses().size() == 1;
            AnnotateImageResponse response = batchResponse.getResponses().get(0);
            if (response.getLabelAnnotations() == null) {
                throw new IOException(
                        response.getError() != null
                                ? response.getError().getMessage()
                                : "Unknown error getting image annotations");
            }
            return response.getLabelAnnotations();
            // [END parse_response]
        }
    }

    private static class CountLabels extends com.google.cloud.dataflow.sdk.transforms.PTransform<PCollection<TableRow>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> apply(PCollection<TableRow> input) {

            //images urls
            final PCollection<URL> urls = input.apply(ParDo.of(new GetUrls()));

            //cloud vision labels
            final PCollection<String> labels = urls.apply(ParDo.of(new GetLabels()));

            // Count the number of times each word occurs.
            final PCollection<KV<String, Long>> wordCounts =
                    labels.apply(Count.perElement());

            return wordCounts;
        }
    }
}
