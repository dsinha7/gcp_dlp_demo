package com.dsinha.dlp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.base.Splitter;
import com.google.privacy.dlp.v2.*;
import com.google.type.Date;

public class ProcessWithDLP {

    public static void main(String[] args) throws Exception {
        // TODO: Replace these variables
        String projectId = "dlp-demo-302116";
        //dlp-demo-302116
        Path inputCsvFile = Paths.get(System.getProperty("user.dir") + "/src/main/resources/iput_data.csv");
        Path outputCsvFile = Paths.get(System.getProperty("user.dir") + "/src/main/resources/output_data.csv");
        System.out.println(inputCsvFile);
        deIdentifyWithDLP(projectId, inputCsvFile, outputCsvFile);
    }

    private static void deIdentifyWithDLP(String projectId, Path inputCsvFile, Path outputCsvFile) {

        try(DlpServiceClient dlp=DlpServiceClient.create()){
            List<FieldId> headers;
            List<Table.Row> rows;

            try(BufferedReader input = Files.newBufferedReader(inputCsvFile)){
                headers = Arrays.stream(input.readLine().split(","))
                    .map(header->FieldId.newBuilder().setName(header).build())
                    .collect(Collectors.toList());

                rows = input.lines()
                        .map(ProcessWithDLP::parseLineAsRow)
                        .collect(Collectors.toList());
            }

            Table table = Table.newBuilder().addAllHeaders(headers).addAllRows(rows).build();
            ContentItem item = ContentItem.newBuilder().setTable(table).build();

            DateShiftConfig dateShiftConfig =
                    DateShiftConfig.newBuilder().setLowerBoundDays(5).setUpperBoundDays(5).build();
            PrimitiveTransformation transformation =
                    PrimitiveTransformation.newBuilder().setDateShiftConfig(dateShiftConfig).build();


            List<FieldId> dateFields = Arrays.asList(headers.get(1), headers.get(3));
            FieldTransformation fieldTransformation =
                    FieldTransformation.newBuilder()
                            .addAllFields(dateFields)
                            .setPrimitiveTransformation(transformation)
                            .build();

            //masking for credit card
            CharacterMaskConfig characterMaskConfig =
                    CharacterMaskConfig.newBuilder().setMaskingCharacter("*")
                            .build();
            PrimitiveTransformation creditCardTransformation  =
                    PrimitiveTransformation.newBuilder().setCharacterMaskConfig(characterMaskConfig).build();
            List<FieldId> cardFields = Arrays.asList(headers.get(2));

            FieldTransformation cardFieldTransformation =
                    FieldTransformation.newBuilder()
                            .addAllFields(cardFields)
                            .setPrimitiveTransformation(creditCardTransformation)
                            .build();



            RecordTransformations recordTransformations =
                    RecordTransformations.newBuilder()
                            .addFieldTransformations(fieldTransformation)
                            .addFieldTransformations(cardFieldTransformation).build();
            // Specify the config for the de-identify request
            DeidentifyConfig deidentifyConfig =
                    DeidentifyConfig.newBuilder().setRecordTransformations(recordTransformations).build();

            // Combine configurations into a request for the service.
            DeidentifyContentRequest request =
                    DeidentifyContentRequest.newBuilder()
                            .setParent(LocationName.of(projectId, "global").toString())
                            .setItem(item)
                            .setDeidentifyConfig(deidentifyConfig)
                            .build();



            // Send the request and receive response from the service
            DeidentifyContentResponse response = dlp.deidentifyContent(request);

            try (BufferedWriter writer = Files.newBufferedWriter(outputCsvFile)) {
                Table outTable = response.getItem().getTable();
                String headerOut =
                        outTable.getHeadersList().stream()
                                .map(FieldId::getName)
                                .collect(Collectors.joining(","));
                writer.write(headerOut + "\n");

                List<String> rowOutput =
                        outTable.getRowsList().stream()
                                .map(row -> joinRow(row.getValuesList()))
                                .collect(Collectors.toList());
                for (String line : rowOutput) {
                    writer.write(line + "\n");
                }
                System.out.println("Content written to file: " + outputCsvFile.toString());
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

        return;
    }

    public static Table.Row parseLineAsRow(String line) {
        List<String> values = Splitter.on(",").splitToList(line);
        Value name = Value.newBuilder().setStringValue(values.get(0)).build();
        Value birthDate = Value.newBuilder().setDateValue(parseAsDate(values.get(1))).build();
        Value creditCardNumber = Value.newBuilder().setStringValue(values.get(2)).build();
        Value registerDate = Value.newBuilder().setDateValue(parseAsDate(values.get(3))).build();
        return Table.Row.newBuilder()
                .addValues(name)
                .addValues(birthDate)
                .addValues(creditCardNumber)
                .addValues(registerDate)
                .build();
    }

    public static Date parseAsDate(String s) {
        LocalDate date = LocalDate.parse(s, DateTimeFormatter.ofPattern("MM/dd/yyyy"));
        return Date.newBuilder()
                .setDay(date.getDayOfMonth())
                .setMonth(date.getMonthValue())
                .setYear(date.getYear())
                .build();
    }

    public static String joinRow(List<Value> values) {
        String name = values.get(0).getStringValue();
        String birthDate = formatDate(values.get(1).getDateValue());
        String creditCardNumber = values.get(2).getStringValue();
        String registerDate = formatDate(values.get(3).getDateValue());
        return String.join(",", name, birthDate, creditCardNumber, registerDate);
    }

    public static String formatDate(Date d) {
        return String.format("%s/%s/%s", d.getMonth(), d.getDay(), d.getYear());
    }
}
