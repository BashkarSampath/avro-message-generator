package com.busakie.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@RestController
@RequestMapping("/avro")
@SpringBootApplication
public class MainApplication {
	public static void main(String[] args) {
		SpringApplication.run(MainApplication.class, args);
	}

	@Bean
	@Primary
	public OpenAPI injectOpenApi() {
		return new OpenAPI().info(new Info().title("com.buskie.avro-message-generator"));
	}

	@PostMapping("/data/encoded-json")
	public ResponseEntity<String> getAvroEncodedJSON(@RequestBody RequestModel input) throws IOException {
		byte[] avroJsonBytes = this.generateAvroJsonBytes(input);
		String avroJsonString = new String(avroJsonBytes);
		log.info("AvroEncodedJson: {}", avroJsonString);
		return ResponseEntity.ok(avroJsonString);
	}

	@PostMapping("/data/generic-record-json")
	public ResponseEntity<String> getAvroEncodedGenericRecord(@RequestBody RequestModel input) throws IOException {
		Schema avroSchema = ReflectData.get().getSchema(RequestModel.class);
		byte[] avroJsonBytes = this.generateAvroJsonBytes(input);
		GenericRecord genericRecord = this.convertJsonToAvroObject(avroJsonBytes, avroSchema);
		String genericRecordString = genericRecord.toString();
		log.info("AvroGenericRecordToString: {}", genericRecordString);
		return ResponseEntity.ok(genericRecordString);
	}

	@GetMapping("/schema")
	public ResponseEntity<String> getAvroEncodedSchemaJSON() {
		Schema avroSchema = ReflectData.get().getSchema(RequestModel.class);
		String scehmaJson = avroSchema.toString(false);
		log.info("AvroSchemaJson: {}", scehmaJson);
		return ResponseEntity.status(HttpStatus.OK).body(scehmaJson);
	}

	@GetMapping("/schema/download")
	public ResponseEntity<ByteArrayResource> downloadAvroEncodedSchemaJSON() {
		Schema schema = ReflectData.get().getSchema(RequestModel.class);
		byte[] schemaBytes = schema.toString(false).getBytes();
		ByteArrayResource resource = new ByteArrayResource(schemaBytes);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
		headers.setContentDispositionFormData("attachment", "requestModel.avsc");
		headers.setContentLength(schemaBytes.length);
		return ResponseEntity.status(HttpStatus.OK).headers(headers).body(resource);
	}

	@Operation(summary = "Avro Message File Download")
	@PostMapping("/message/download")
	public ResponseEntity<ByteArrayResource> downloadAvroMessages(@RequestBody RequestModel input) throws IOException {
		Schema avroSchema = ReflectData.get().getSchema(RequestModel.class);
		byte[] avroEncodedJsonBytes = this.generateAvroJsonBytes(input);

		/* Convert Avro-encoded JSON to Generic Record for handling Union types */
		GenericRecord genericRecord = this.convertJsonToAvroObject(avroEncodedJsonBytes, avroSchema);
		byte[] avroBytes = this.convertToAvroMessage(genericRecord, avroSchema);

		ByteArrayResource resource = new ByteArrayResource(avroBytes);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=avro_message.avro");
		headers.setContentLength(avroBytes.length);
		return ResponseEntity.status(HttpStatus.OK).headers(headers).body(resource);
	}

	private GenericRecord convertJsonToAvroObject(byte[] avroEncodedJsonBytes, Schema avroSchema) throws IOException {
		JsonDecoder decoder = DecoderFactory.get().jsonDecoder(avroSchema,
				new ByteArrayInputStream(avroEncodedJsonBytes));
		DatumReader<org.apache.avro.generic.GenericRecord> reader = new GenericDatumReader<>(avroSchema);
		return reader.read(null, decoder);
	}

	private final byte[] convertToAvroMessage(GenericRecord avroRecord, Schema avroSchema) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
		try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			// Write Avro schema and record into Avro message file
			dataFileWriter.create(avroSchema, outputStream);
			dataFileWriter.append(avroRecord);
			dataFileWriter.flush();
			return outputStream.toByteArray();
		}
	}

	private final byte[] generateAvroJsonBytes(@lombok.NonNull RequestModel requestModel) throws IOException {
		Schema schema = ReflectData.get().getSchema(RequestModel.class);

		/* Serialize the object into Avro JSON format */
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DatumWriter<RequestModel> datumWriter = new ReflectDatumWriter<>(schema);
		Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
		datumWriter.write(requestModel, jsonEncoder);
		jsonEncoder.flush();
		return outputStream.toByteArray();
	}
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class RequestModel {
	@io.swagger.v3.oas.annotations.media.Schema(example = "Bashkar Sampath")
	private String name;

	@io.swagger.v3.oas.annotations.media.Schema(example = "27")
	private Integer age;

	@Nullable
	@io.swagger.v3.oas.annotations.media.Schema(example = "")
	private Long phoneNumber;
}