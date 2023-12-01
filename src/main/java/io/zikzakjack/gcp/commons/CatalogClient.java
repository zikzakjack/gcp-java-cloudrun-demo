package io.zikzakjack.gcp.commons;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.threeten.bp.Duration;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.RetrySettings.Builder;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datacatalog.v1.DataCatalogClient;
import com.google.cloud.datacatalog.v1.DataCatalogSettings;

public class CatalogClient {

	public static DataCatalogClient create() throws Exception {
		return DataCatalogClient.create(dataCatalogSettings());
	}

	private static RetrySettings dataCatalogRetrySettings() {
		Builder builder = RetrySettings.newBuilder();
		builder.setRetryDelayMultiplier(1.3);
		builder.setInitialRetryDelay(Duration.ofMillis(100L));
		builder.setMaxRetryDelay(Duration.ofMillis(540000L)); // 9 mins
		builder.setTotalTimeout(Duration.ofMillis(540000L)); // 9 mins
		return builder.build();
	}

	private static HashSet<StatusCode.Code> dataCatalogRetryableCodes() {
		return new HashSet<>(Arrays.asList(StatusCode.Code.RESOURCE_EXHAUSTED, StatusCode.Code.INTERNAL,
				StatusCode.Code.UNAVAILABLE));
	}

	private static DataCatalogSettings dataCatalogSettings() throws IOException {
		RetrySettings retrySettings = dataCatalogRetrySettings();
		HashSet<StatusCode.Code> retryableCodes = dataCatalogRetryableCodes();
		DataCatalogSettings.Builder builder = DataCatalogSettings.newBuilder();
		builder.searchCatalogSettings().setRetrySettings(retrySettings).setRetryableCodes(retryableCodes);
		builder.listTagsSettings().setRetrySettings(retrySettings).setRetryableCodes(retryableCodes);
		builder.lookupEntrySettings().setRetrySettings(retrySettings).setRetryableCodes(retryableCodes);
		DataCatalogSettings dataCatalogSettings = builder.build();
		return dataCatalogSettings;
	}

}
