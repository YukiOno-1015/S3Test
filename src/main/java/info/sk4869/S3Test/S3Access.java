package info.sk4869.S3Test;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * AWS S3バケットにアクセスして操作を行うためのクラス。
 */
public class S3Access {

	private static final String ENDPOINT_URL = "http://192.168.1.36:9000";
	private static final String ACCESS_KEY = "YYEneHFhf3tFLrTf";
	private static final String SECRET_KEY = "5tTnyJeentEAHJeJ";

	/**
	 * 指定されたバケットにオブジェクトをアップロードする。
	 *
	 * @param bucketName S3バケット名
	 * @param objectKey  アップロードするオブジェクトのキー
	 * @param objectSize アップロードするオブジェクトのサイズ
	 * @param is         アップロードするオブジェクトのInputStream
	 * @throws Exception S3操作中に発生した例外
	 */
	public void putObject(String bucketName, String objectKey, int objectSize, InputStream is) throws Exception {
		AmazonS3 client = getClient(bucketName);

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(objectSize);

		client.putObject(bucketName, objectKey, is, metadata);
	}

	/**
	 * 指定されたバケットからオブジェクトをダウンロードする。
	 *
	 * @param bucketName S3バケット名
	 * @param objectKey  ダウンロードするオブジェクトのキー
	 * @return S3ObjectInputStream ダウンロードしたオブジェクトのInputStream
	 * @throws Exception S3操作中に発生した例外
	 */
	public S3ObjectInputStream getObject(String bucketName, String objectKey) throws Exception {
		AmazonS3 client = getClient(bucketName);

		S3Object s3Object = client.getObject(bucketName, objectKey);
		return s3Object.getObjectContent();
	}

	/**
	 * 指定されたバケットから指定されたキーのオブジェクトを一括削除する。
	 *
	 * @param bucketName  S3バケット名
	 * @param objectKeys 削除するオブジェクトのキーリスト
	 * @return List<String> 削除されたオブジェクトのキーリスト
	 * @throws Exception S3操作中に発生した例外
	 */
	public List<String> deleteObjects(String bucketName, List<String> objectKeys) throws Exception {
		AmazonS3 client = getClient(bucketName);

		List<KeyVersion> keys = new ArrayList<>();
		objectKeys.forEach(obj -> keys.add(new KeyVersion(obj)));

		DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
		DeleteObjectsResult result = client.deleteObjects(request);

		List<String> deleted = new ArrayList<>();
		result.getDeletedObjects().forEach(obj -> deleted.add(obj.getKey()));

		return deleted;
	}

	/**
	 * 指定されたバケット内のオブジェクト一覧を表示する。
	 *
	 * @param bucketName S3バケット名
	 * @throws Exception S3操作中に発生した例外
	 */
	public void show(String bucketName) throws Exception {
		AmazonS3 client = getClient(bucketName);

		ObjectListing objListing = client.listObjects(bucketName);
		List<S3ObjectSummary> objList = objListing.getObjectSummaries();

		for (S3ObjectSummary obj : objList) {
			TimeZone timeZoneJP = TimeZone.getTimeZone("Asia/Tokyo");
			SimpleDateFormat fmt = new SimpleDateFormat();
			fmt.setTimeZone(timeZoneJP);
			System.out.println("Key [" + obj.getKey() + "] / Size [" + obj.getSize() + " B] / Last Modified ["
					+ fmt.format(obj.getLastModified()) + "]");
		}
	}

	/**
	 * 指定されたバケット名に基づいてAmazonS3クライアントを生成する。
	 *
	 * @param bucketName S3バケット名
	 * @return AmazonS3 生成されたS3クライアント
	 * @throws Exception S3操作中に発生した例外
	 */
	private AmazonS3 getClient(String bucketName) throws Exception {
		AWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTPS);
		clientConfig.setConnectionTimeout(10000);

		EndpointConfiguration endpointConfiguration = new EndpointConfiguration(ENDPOINT_URL, Regions.US_EAST_1.name());

		AmazonS3 client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withClientConfiguration(clientConfig)
				.withEndpointConfiguration(endpointConfiguration).build();

		if (!client.doesBucketExist(bucketName)) {
			throw new Exception("S3バケット[" + bucketName + "]がありません");
		}

		return client;
	}
}
