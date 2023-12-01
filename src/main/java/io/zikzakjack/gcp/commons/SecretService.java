package io.zikzakjack.gcp.commons;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SecretService {

	static String logMsg = "[DataOps] [Secret] Retrieving secret succeeded. myProject: {} secretId : {}, secretVersionId : {}, secret : {}";
	static String errMsg = "[DataOps] [Secret] Retrieving secret failed. Reason : {}, myProject: {} secretId : {}, secretVersionId : {}";

	public static String getSecret(String myProject, String secretId, String secretVersionId) {

		String secret = null;
		try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
			SecretVersionName secretVersionName = SecretVersionName.of(myProject, secretId, secretVersionId);
			AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
			secret = response.getPayload().getData().toStringUtf8();

			if (secret == null || "".equals(secret)) {
				throw new RuntimeException("Retrieving secret failed");
			}

			log.info(logMsg, myProject, secretId, secretVersionId, secret);
			return secret;
		} catch (Exception ex) {
			log.error(errMsg, ex.toString(), myProject, secretId, secretVersionId);
			throw new RuntimeException(ex);
		}

	}

}
