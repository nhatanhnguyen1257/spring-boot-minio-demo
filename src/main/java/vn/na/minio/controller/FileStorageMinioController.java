package vn.na.minio.controller;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import vn.na.minio.service.FileStorageMinioService;

@RestController
@RequestMapping("/minio-control/")
public class FileStorageMinioController {

	@Autowired
	private FileStorageMinioService fileStorageMinioService;
	
	@PostMapping("upload-files")
	public Object uploadFile(MultipartFile []files) throws InvalidKeyException, 
															ErrorResponseException, 
															InsufficientDataException, 
															InternalException, 
															InvalidResponseException, 
															NoSuchAlgorithmException, 
															ServerException, 
															XmlParserException, 
															IllegalArgumentException, 
															IOException {
		fileStorageMinioService.uploadFile(files);
	
		return "upload-thành công";
	}
	
	@PostMapping("upload-files-async")
	public Object uploadFileAsync(MultipartFile []files) throws InvalidKeyException, 
																ErrorResponseException, 
																InsufficientDataException, 
																InternalException, 
																InvalidResponseException, 
																NoSuchAlgorithmException, 
																ServerException, 
																XmlParserException, 
																IllegalArgumentException, 
																IOException {
		fileStorageMinioService.uploadFileAsync(files);
		return "upload-thành công";
	}
	
	@DeleteMapping("delete")
	public Object uploadFileAsync(@RequestParam(name = "objs")String objects) throws InvalidKeyException, 
																ErrorResponseException, 
																InsufficientDataException, 
																InternalException, 
																InvalidResponseException, 
																NoSuchAlgorithmException, 
																ServerException, 
																XmlParserException, 
																IllegalArgumentException, 
																IOException {
		fileStorageMinioService.deleteObject(Arrays.asList(objects));
		return "xóa thành công";
	}
	
	@GetMapping("download")
	public Object uploadFileAsync(@RequestParam(name = "objs")String objects,
									@RequestParam(name = "fileName", required = false)String fileName) throws InvalidKeyException, 
																ErrorResponseException, 
																InsufficientDataException, 
																InternalException, 
																InvalidResponseException, 
																NoSuchAlgorithmException, 
																ServerException, 
																XmlParserException, 
																IllegalArgumentException, 
																IOException {
		fileStorageMinioService.download(objects, fileName);
		return "Download thành công";
	}
	
}
