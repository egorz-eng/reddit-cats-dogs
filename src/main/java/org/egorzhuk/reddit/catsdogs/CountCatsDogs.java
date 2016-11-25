package org.egorzhuk.reddit.catsdogs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.IOUtils;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.vision.v1.Vision;
import com.google.api.services.vision.v1.VisionScopes;
import com.google.api.services.vision.v1.model.*;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableList;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

/**
 * The pipeline
 */
public class CountCatsDogs {

    static final String REDDIT_IMGUR_CATS_DOGS = "bq-playground-1366:reddit.imgur_cats_dogs";
    static final String REDDIT_IMGUR_CATS_DOGS_RES = "bq-playground-1366:reddit.cats_dogs_result";

    public static void main(String[] args) {
        //Read cmd arguments and use custom options
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        //create pipeline
        final Pipeline pipeline = Pipeline.create(options);

        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("description").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);

        pipeline.apply(BigQueryIO.Read.from(options.getInput()))
                .apply(new CountLabels())
                .apply(BigQueryIO.Write
                        .to(options.getOutput())
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        pipeline.run();
    }

    private static interface Options extends DataflowPipelineOptions {
        @Description("Table to read from, specified as "
                + "<project_id>:<dataset_id>.<table_id>")
        @Default.String(REDDIT_IMGUR_CATS_DOGS)
        String getInput();

        void setInput(String value);

        @Description("BigQuery table to write to, specified as "
                + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
        @Default.String(REDDIT_IMGUR_CATS_DOGS_RES)
        String getOutput();

        void setOutput(String value);
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


    public static class GetUrls extends com.google.cloud.dataflow.sdk.transforms.DoFn<TableRow, URL> {

        public static final String IMGUR_BASEURL = "http://i.imgur.com/";

        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            final TableRow row = processContext.element();
            String res = (String) row.get("url");
            try {
                if (!res.contains("i.imgur.com")) {
                    final String[] split = res.split("/");
                    res = IMGUR_BASEURL + split[split.length - 1] + ".png";
                }
                final URL url = new URL(res);
                processContext.output(url);
            } catch (Exception e) {
                //No output
            }
        }
    }

    public static class GetLabels extends com.google.cloud.dataflow.sdk.transforms.DoFn<URL, String> {


        @Override
        public void processElement(ProcessContext processContext) throws Exception {
            final Vision vision = connect(processContext.getPipelineOptions().as(Options.class).getProject());
            final URL url = processContext.element();
            List<EntityAnnotation> entityAnnotations = new ArrayList<>();
            try {
                final byte[] imageBytes = getImageBytes(url);
                entityAnnotations = labelImage(vision, imageBytes);
            } catch (Exception e) {
                //skip it
            }

            for (EntityAnnotation a : entityAnnotations) {
                processContext.output(a.getDescription());
            }
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

    public static class CountLabels extends com.google.cloud.dataflow.sdk.transforms.PTransform<PCollection<TableRow>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> apply(PCollection<TableRow> input) {

            //images urls
            final PCollection<URL> urls = input.apply(ParDo.of(new GetUrls()));

            //cloud vision labels
            final PCollection<String> labels = urls.apply(ParDo.of(new GetLabels()));

            // Count the number of times each word occurs.
            final PCollection<KV<String, Long>> wordCounts =
                    labels.apply(Count.<String>perElement());

            //Get resulting rows
            final PCollection<TableRow> res = wordCounts.apply(ParDo.of(new ToRow()));

            return res;
        }

        private class ToRow extends DoFn<KV<String, Long>, TableRow> {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                TableRow row = new TableRow()
                        .set("description", c.element().getKey())
                        .set("count", c.element().getValue());
                c.output(row);
            }
        }
    }
}
