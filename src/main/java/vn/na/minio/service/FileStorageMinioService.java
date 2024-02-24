package vn.na.minio.service;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.GenericResponse;
import io.minio.MakeBucketArgs;
import io.minio.MinioAsyncClient;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.SnowballObject;
import io.minio.UploadObjectArgs;
import io.minio.UploadSnowballObjectsArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import jdk.jfr.ContentType;

@Service
public class FileStorageMinioService {

	@Value("${spring.minio.url}")
	private String minioUrl;
	@Value("${spring.minio.bucket}")
	private String minioBucket;
	@Value("${spring.minio.access-key}")
	private String minioAccessKey;
	@Value("${spring.minio.secret-key}")
	private String minioSecretKey;

	public List<ObjectWriteResponse> uploadFile(MultipartFile[] files) throws InvalidKeyException, ErrorResponseException,
			InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException,
			ServerException, XmlParserException, IllegalArgumentException, IOException {
		MinioClient minioClient = getMinioClient();
		List<ObjectWriteResponse> lst = new ArrayList<ObjectWriteResponse>();
		for (MultipartFile file : files) {
			lst.add(minioClient.putObject(PutObjectArgs.builder()
					.bucket(minioBucket)
					.object(file.getOriginalFilename())
					.stream(file.getInputStream(), file.getSize(), -1)
					.build()));
		}
		
		return lst;

	}

	public List<CompletableFuture<ObjectWriteResponse>> uploadFileAsync(MultipartFile[] files) throws InvalidKeyException,
			ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException,
			NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException {
		MinioAsyncClient minioClient = getMinioAsyncClient();
		
		List<CompletableFuture<ObjectWriteResponse>> lst = new ArrayList<CompletableFuture<ObjectWriteResponse>>();
		for (MultipartFile file : files) {
			lst.add(minioClient.putObject(PutObjectArgs.builder()
					.bucket(minioBucket)
					.object(file.getOriginalFilename())
					.stream(file.getInputStream(), file.getSize(), -1)
					.build()));;
		}
	
		return lst;
	}
	
	
	public void deleteObject(List<String> lstObject) throws InvalidKeyException, InsufficientDataException,
			InternalException, NoSuchAlgorithmException, XmlParserException, IllegalArgumentException,
			ErrorResponseException, InvalidResponseException, ServerException, IOException {
		List<DeleteObject> objects = new LinkedList<>();
		lstObject.forEach(item->{
			objects.add(new DeleteObject(item));
		});
		MinioClient minioClient = getMinioClient();
		
		Iterable<Result<DeleteError>> results =
		minioClient.removeObjects(
			        RemoveObjectsArgs.builder().bucket(minioBucket).objects(objects).build());
		
		for (Result<DeleteError> result : results) {
			  DeleteError error = result.get();
			  System.out.println(
			      "Error in deleting object " + error.objectName() + "; " + error.message());
			}
	}
	
	
	public void download(String objectName, String fileName) throws InvalidKeyException, InsufficientDataException,
			InternalException, NoSuchAlgorithmException, XmlParserException, IllegalArgumentException,
			ErrorResponseException, InvalidResponseException, ServerException, IOException {
		if (fileName == null) {
			fileName = objectName;
		}
		MinioClient minioClient = getMinioClient();
		minioClient.downloadObject(
				  DownloadObjectArgs.builder()
				  .bucket(minioBucket)
				  .object(objectName)
				  .filename(fileName)
				  .build());
	}

	private MinioClient getMinioClient() throws InvalidKeyException, InsufficientDataException, InternalException,
			NoSuchAlgorithmException, XmlParserException, IllegalArgumentException, IOException, ErrorResponseException,
			InvalidResponseException, ServerException {

		MinioClient minioClient = MinioClient.builder().
				endpoint(minioUrl)
				.credentials(minioAccessKey, minioSecretKey)
				.build();

		// Make 'asiatrip' bucket if not exist.
		boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(minioBucket).build());
		if (!found) {
			// Make a new bucket called 'asiatrip'.
			minioClient.makeBucket(MakeBucketArgs.builder().bucket(minioBucket).build());
		}

		return minioClient;
	}

	private MinioAsyncClient getMinioAsyncClient() throws InvalidKeyException, InsufficientDataException,
			InternalException, NoSuchAlgorithmException, XmlParserException, IllegalArgumentException, IOException {
		MinioAsyncClient minioClient = MinioAsyncClient.builder().endpoint(minioUrl)
				.credentials(minioAccessKey, minioSecretKey).build();

		// Make 'asiatrip' bucket if not exist.
		boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(minioBucket).build()) != null;
		if (!found) {
			// Make a new bucket called 'asiatrip'.
			minioClient.makeBucket(MakeBucketArgs.builder().bucket(minioBucket).build());
		}

		return minioClient;
	}
	
}
