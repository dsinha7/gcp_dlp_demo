package com.dsinha.dlp;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.base.Splitter;
import com.google.privacy.dlp.v2.*;
import com.google.protobuf.Descriptors;
import com.google.type.Date;
import io.grpc.internal.JsonUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Collator;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InspectWithDLP {

    public static void main(String[] args) throws Exception {
        // TODO: Replace these variables
        String projectId = "dlp-demo-302116";
        Path inputCsvFile = Paths.get("/Users/dsinha/work/java/intelij-ws/dlp-exmaple/customer_data_with_org.csv");
        Path outputCsvFile = Paths.get(System.getProperty("user.dir") + "/src/main/resources/output_data.csv");
        System.out.println(inputCsvFile);
        inspectWithDLPTableAPI(projectId, inputCsvFile, outputCsvFile);
    }

    private static void inspectWithDLPTableAPI(String projectId, Path inputCsvFile, Path outputCsvFile) {

        try(DlpServiceClient dlp =DlpServiceClient.create()){
            List<FieldId> headers;
            List<Table.Row> rows;

            try(BufferedReader input = Files.newBufferedReader(inputCsvFile)){
                headers = Arrays.stream(input.readLine().split(","))
                    .map(header->FieldId.newBuilder().setName(header).build())
                    .collect(Collectors.toList());

                rows = input.lines()
                        .map(InspectWithDLP::parseLineAsRow)
                        .collect(Collectors.toList());
            }

            Table table = Table.newBuilder().addAllHeaders(headers).addAllRows(rows).build();
            ContentItem item = ContentItem.newBuilder().setTable(table).build();

            List<InfoType> infoTypes = Stream.of("FIRST_NAME","EMAIL_ADDRESS","AGE")
                    .map(it -> InfoType.newBuilder().setName(it).build())
                    .collect(Collectors.toList());

            String customRegexPattern = "(300|3)\\d{3,4}";
            String orgIdregex = "(100|1)\\d{8,9}";

            Map<String, String> regexs = new HashMap<>();
            regexs.put("CUSTOMER_ID", customRegexPattern);
            regexs.put("ORG_ID", orgIdregex);

            //custom types
            List<CustomInfoType> customInfoTypes = Stream.of("CUSTOMER_ID","ORG_ID").map(
                    ctype -> CustomInfoType.newBuilder()
                            .setInfoType(InfoType.newBuilder().setName(ctype).build())
                            .setRegex(CustomInfoType.Regex.newBuilder()
                                    .setPattern(regexs.get(ctype)).build()).build()
            ).collect(Collectors.toList());


            InspectConfig.FindingLimits findingLimits = InspectConfig.FindingLimits.newBuilder()
                    .setMaxFindingsPerItem(0).build();
            //create inspection configuration
            InspectConfig inspectConfig = InspectConfig.newBuilder()
                    .addAllInfoTypes(infoTypes)
                    .addAllCustomInfoTypes(customInfoTypes)
                    .setMinLikelihood(Likelihood.UNLIKELY)
                    .setLimits(findingLimits)
                    .setIncludeQuote(true).build();

            // Combine configurations into a request for the service.
            InspectContentRequest request =
                    InspectContentRequest.newBuilder()
                            .setParent(LocationName.of(projectId, "global").toString())
                            .setItem(item)
                            .setInspectConfig(inspectConfig)
                            .build();



            // Send the request and receive response from the service
            InspectContentResponse response = dlp.inspectContent(request);

            System.out.println(response);
            //Stream.of("FIRST_NAME","EMAIL_ADDRESS","AGE", "CUSTOMER_ID","ORG_ID")

            System.out.println();
            System.out.println();
            System.out.println();

            System.out.println("finding count" + response.getResult().getFindingsCount());
            response.getResult().getFindingsList().forEach(s -> {
                String finding = " Column Name :[" + s.getLocation().getContentLocationsList().get(0).getRecordLocation().getFieldId().getName() +"]"
                + " InfoType :" + s.getInfoType().getName()
                        +", Likelihood :" + s.getLikelihood()
                        +", Field Data :" + s.getQuote();



                System.out.println(finding);

            });


//            try (BufferedWriter writer = Files.newBufferedWriter(outputCsvFile)) {
//                Table outTable = response.getItem().getTable();
//                String headerOut =
//                        outTable.getHeadersList().stream()
//                                .map(FieldId::getName)
//                                .collect(Collectors.joining(","));
//                writer.write(headerOut + "\n");
//
//                List<String> rowOutput =
//                        outTable.getRowsList().stream()
//                                .map(row -> joinRow(row.getValuesList()))
//                                .collect(Collectors.toList());
//                for (String line : rowOutput) {
//                    writer.write(line + "\n");
//                }
//                System.out.println("Content written to file: " + outputCsvFile.toString());
//            }



        } catch (IOException e) {
            e.printStackTrace();
        }

        return;
    }

    public static Table.Row parseLineAsRow(String line) {
        //customer id,name,email ,org id
        List<String> values = Splitter.on(",").splitToList(line);
        Table.Row.Builder builder = Table.Row.newBuilder();
        for (int i = 0; i < values.size(); i++) {
            builder.addValues(Value.newBuilder().setStringValue(values.get(i)).build());
        }
        return builder.build();
    }


//    public static String joinRow(List<Value> values) {
//        String name = values.get(0).getStringValue();
//        String birthDate = formatDate(values.get(1).getDateValue());
//        String creditCardNumber = values.get(2).getStringValue();
//        String registerDate = formatDate(values.get(3).getDateValue());
//        return String.join(",", name, birthDate, creditCardNumber, registerDate);
//    }

    public static String formatDate(Date d) {
        return String.format("%s/%s/%s", d.getMonth(), d.getDay(), d.getYear());
    }
}
