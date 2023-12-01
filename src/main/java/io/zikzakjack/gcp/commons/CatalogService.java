package io.zikzakjack.gcp.commons;

import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_LOOKUP_DATASET;
import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_LOOKUP_PROJECT;
import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_LOOKUP_VIEW;
import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_TAG_TMPL_COLUMN;
import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_TAG_TMPL_COLUMN_V2;
import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_TAG_TMPL_DATASET;
import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_TAG_TMPL_VIEW;
import static io.zikzakjack.gcp.commons.CatalogConstants.CTLG_TAG_TMPL_VIEW_V2;
import static io.zikzakjack.gcp.commons.CatalogConstants.MD_DT_DMN;
import static io.zikzakjack.gcp.commons.CatalogConstants.MD_RSK_CTGRY;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.cloud.datacatalog.v1.DataCatalogClient;
import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.ListTagsRequest;
import com.google.cloud.datacatalog.v1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1.Tag;
import com.google.cloud.datacatalog.v1.TagField;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CatalogService {

	static String logCtlgLookupProjectStart = "[DataOps] [DataCatalog] [Project] processing. parentProject : {}";
	static String logCtlgLookupProjectFinish = "[DataOps] [DataCatalog] [Project] processed. parentProject : {}, projectMap : {}";
	static String logCtlgLookupProjectFail = "[DataOps] [DataCatalog] [Project] [FATAL] failed. parentProject : {}, projectMap : {}, exception : {}";
	static String logCtlgLookupProjectDebug = "[DataOps] [DataCatalog] [Project] [DEBUG] parentProject : {}, key : {}, value : {}";

	static String logCtlgLookupProjectV2Start = "[DataOps] [DataCatalog] [Project] [V2] processing. parentProject : {}, catalogProject : {}, entryGroup : {}";
	static String logCtlgLookupProjectV2Finish = "[DataOps] [DataCatalog] [Project] [V2] processed. parentProject : {}, projectMap : {}, catalogProject : {}, entryGroup : {}";
	static String logCtlgLookupProjectV2Fail = "[DataOps] [DataCatalog] [Project] [V2] [FATAL] failed. parentProject : {}, projectMap : {}, exception : {}, catalogProject : {}, entryGroup : {}";
	static String logCtlgLookupProjectV2Debug = "[DataOps] [DataCatalog] [Project] [V2] [DEBUG] parentProject : {}, key : {}, value : {}, catalogProject : {}, entryGroup : {}";

	static String logCtlgLookupDatasetStart = "[DataOps] [DataCatalog] [Dataset] processing. parentDataset : {}";
	static String logCtlgLookupDatasetFinish = "[DataOps] [DataCatalog] [Dataset] processed. parentDataset : {}, datasetMap : {}";
	static String logCtlgLookupDatasetFail = "[DataOps] [DataCatalog] [Dataset] [FATAL] failed. parentDataset : {}, datasetMap : {}, exception : {}";
	static String logCtlgLookupDatasetDebug = "[DataOps] [DataCatalog] [Dataset] [DEBUG] parentDataset : {}, key : {}, value : {}";

	static String logCtlgLookupViewStart = "[DataOps] [DataCatalog] [View] processing. parentView : {}";
	static String logCtlgLookupViewFinish = "[DataOps] [DataCatalog] [View] processed. parentView : {}, viewMap : {}";
	static String logCtlgLookupViewFail = "[DataOps] [DataCatalog] [View] [FATAL] failed. parentView : {}, viewMap : {}, exception : {}";
	static String logCtlgLookupViewDebug = "[DataOps] [DataCatalog] [View] [DEBUG] parentView : {}, key : {}, value : {}";

	static String logCtlgLookupViewV2Start = "[DataOps] [DataCatalog] [View] [V2] processing. parentView : {}";
	static String logCtlgLookupViewV2Finish = "[DataOps] [DataCatalog] [View] [V2] processed. parentView : {}, viewMap : {}";
	static String logCtlgLookupViewV2Fail = "[DataOps] [DataCatalog] [View] [V2] [FATAL] failed. parentView : {}, viewMap : {}, exception : {}";
	static String logCtlgLookupViewV2Debug = "[DataOps] [DataCatalog] [View] [V2] [DEBUG] parentView : {}, key : {}, value : {}";

	static String logCtlgLookupColumnStart = "[DataOps] [DataCatalog] [Column] processing. parentView : {}";
	static String logCtlgLookupColumnFinish1 = "[DataOps] [DataCatalog] [Column] processed. parentView : {}, colwiseMetadataMap : {}";
	static String logCtlgLookupColumnFinish2 = "[DataOps] [DataCatalog] [Column] processed. parentView : {}, catwiseMetadataMap : {}";
	static String logCtlgLookupColumnFail = "[DataOps] [DataCatalog] [Column] [FATAL] failed. parentView : {}, columnMap : {}, exception : {}";
	static String logCtlgLookupColumnDebug = "[DataOps] [DataCatalog] [Column] parentView : {}, tagName : {}, columnName : {}, risk_category : {}, data_domain : {}";

	static String logCtlgLookupColumnV2Start = "[DataOps] [DataCatalog] [Column] [V2] processing. parentView : {}";
	static String logCtlgLookupColumnV2Finish1 = "[DataOps] [DataCatalog] [Column] [V2] processed. parentView : {}, colwiseMetadataMap : {}";
	static String logCtlgLookupColumnV2Finish2 = "[DataOps] [DataCatalog] [Column] [V2] processed. parentView : {}, catwiseMetadataMap : {}";
	static String logCtlgLookupColumnV2Fail = "[DataOps] [DataCatalog] [Column] [V2] [FATAL] failed. parentView : {}, columnMap : {}, exception : {}";
	static String logCtlgLookupColumnV2Debug = "[DataOps] [DataCatalog] [Column] parentView : {}, tagName : {}, columnName : {}, risk_category : {}, data_domain : {}";

	// OTHER CONSTANTS
	static String MD_CNTRY_CLMN_VALUE = "_dfgdia_iso3_country_std_cnty";
	static String MD_CNTRY_ELGBLTY_VALUE = "TRUE";

	public static Map<String, Object> lookupProjectMetadata(String prntProject) throws IOException {

		log.debug(logCtlgLookupProjectStart, prntProject);
		String ctlgProject = prntProject;
		String entryGroup = prntProject.replace("-", "_");
		String entry = prntProject.replace("-", "_");
		Map<String, Object> projectMap = new HashMap<>();

		try (DataCatalogClient client = CatalogClient.create()) {
			String entryName = String.format(CTLG_LOOKUP_PROJECT, ctlgProject, entryGroup, entry);
			ListTagsRequest request = ListTagsRequest.newBuilder().setParent(entryName).setPageSize(1000).build();
			DataCatalogClient.ListTagsPagedResponse response = client.listTags(request);

			for (Tag tag : response.iterateAll()) {
				for (Map.Entry<String, TagField> field : tag.getFieldsMap().entrySet()) {
					String key = field.getKey();
					Object value = getTagFieldValue(field);
					projectMap.put(key, value);
					log.debug(logCtlgLookupProjectDebug, prntProject, key, value);
				}
			}
			log.debug(logCtlgLookupProjectFinish, prntProject, projectMap);
		} catch (Exception ex) {
			log.debug(logCtlgLookupProjectFail, prntProject, projectMap, ex.toString());
			throw new RuntimeException(ex);
		}
		return projectMap;
	}

	public static Map<String, Object> lookupProjectMetadataV2(String catalogProject, String entryGroup,
			String prntProject) throws IOException {

		log.debug(logCtlgLookupProjectV2Start, prntProject, catalogProject, entryGroup);
		String entry = prntProject.replace("-", "_");
		Map<String, Object> projectMap = new HashMap<>();

		try (DataCatalogClient client = CatalogClient.create()) {
			String entryName = String.format(CTLG_LOOKUP_PROJECT, catalogProject, entryGroup, entry);
			ListTagsRequest request = ListTagsRequest.newBuilder().setParent(entryName).setPageSize(1000).build();
			DataCatalogClient.ListTagsPagedResponse response = client.listTags(request);

			for (Tag tag : response.iterateAll()) {
				for (Map.Entry<String, TagField> field : tag.getFieldsMap().entrySet()) {
					String key = field.getKey();
					Object value = getTagFieldValue(field);
					projectMap.put(key, value);
					log.debug(logCtlgLookupProjectV2Debug, prntProject, key, value, catalogProject, entryGroup);
				}
			}
			log.debug(logCtlgLookupProjectV2Finish, prntProject, projectMap, catalogProject, entryGroup);
		} catch (Exception ex) {
			log.debug(logCtlgLookupProjectV2Fail, prntProject, projectMap, ex.toString(), catalogProject, entryGroup);
			throw new RuntimeException(ex);
		}
		return projectMap;
	}

	public static Map<String, Object> lookupDatasetMetadata(String prntProject, String prntDataset) throws IOException {

		String parentDatasetFQN = String.format("%s.%s", prntProject, prntDataset);
		Map<String, Object> datasetMap = new HashMap<>();
		log.debug(logCtlgLookupDatasetStart, parentDatasetFQN);

		try (DataCatalogClient client = CatalogClient.create()) {
			String linkedResource = String.format(CTLG_LOOKUP_DATASET, prntProject, prntDataset);
			LookupEntryRequest lookupRequest = LookupEntryRequest.newBuilder().setLinkedResource(linkedResource)
					.build();
			Entry entry = client.lookupEntry(lookupRequest);
			ListTagsRequest request = ListTagsRequest.newBuilder().setParent(entry.getName()).setPageSize(1000).build();
			DataCatalogClient.ListTagsPagedResponse response = client.listTags(request);

			for (Tag tag : response.iterateAll()) {
				if (tag.getTemplateDisplayName().equals(CTLG_TAG_TMPL_DATASET)) {
					for (Map.Entry<String, TagField> field : tag.getFieldsMap().entrySet()) {
						String key = field.getKey();
						Object value = getTagFieldValue(field);
						datasetMap.put(key, value);
						log.debug(logCtlgLookupDatasetDebug, parentDatasetFQN, key, value);
					}
				}
			}
			log.debug(logCtlgLookupDatasetFinish, parentDatasetFQN, datasetMap);
		} catch (Exception ex) {
			log.debug(logCtlgLookupDatasetFail, parentDatasetFQN, datasetMap, ex.toString());
			throw new RuntimeException(ex);
		}
		return datasetMap;
	}

	public static Map<String, Object> lookupViewMetadata(String prntProject, String prntDataset, String prntView)
			throws IOException {

		String parentViewFQN = String.format("%s.%s.%s", prntProject, prntDataset, prntView);
		Map<String, Object> viewMap = new HashMap<>();
		log.debug(logCtlgLookupViewStart, parentViewFQN);

		try (DataCatalogClient client = CatalogClient.create()) {
			String linkedResource = String.format(CTLG_LOOKUP_VIEW, prntProject, prntDataset, prntView);
			LookupEntryRequest lookupRequest = LookupEntryRequest.newBuilder().setLinkedResource(linkedResource)
					.build();
			Entry entry = client.lookupEntry(lookupRequest);
			ListTagsRequest request = ListTagsRequest.newBuilder().setParent(entry.getName()).setPageSize(1000).build();
			DataCatalogClient.ListTagsPagedResponse response = client.listTags(request);

			for (Tag tag : response.iterateAll()) {
				if (tag.getTemplateDisplayName().equals(CTLG_TAG_TMPL_VIEW)) {
					for (Map.Entry<String, TagField> field : tag.getFieldsMap().entrySet()) {
						String key = field.getKey();
						Object value = getTagFieldValue(field);
						viewMap.put(key, value);
						log.debug(logCtlgLookupViewDebug, parentViewFQN, key, value);
					}
				}
			}
			log.debug(logCtlgLookupViewFinish, parentViewFQN, viewMap);

		} catch (Exception ex) {
			log.debug(logCtlgLookupViewFail, parentViewFQN, viewMap, ex.toString());
			throw new RuntimeException(ex);
		}
		return viewMap;
	}

	public static Map<String, Object> lookupViewMetadataV2(String prntProject, String prntDataset, String prntView)
			throws IOException {

		String parentViewFQN = String.format("%s.%s.%s", prntProject, prntDataset, prntView);
		Map<String, Object> viewMap = new HashMap<>();
		log.debug(logCtlgLookupViewV2Start, parentViewFQN);

		try (DataCatalogClient client = CatalogClient.create()) {
			String linkedResource = String.format(CTLG_LOOKUP_VIEW, prntProject, prntDataset, prntView);
			LookupEntryRequest lookupRequest = LookupEntryRequest.newBuilder().setLinkedResource(linkedResource)
					.build();
			Entry entry = client.lookupEntry(lookupRequest);
			ListTagsRequest request = ListTagsRequest.newBuilder().setParent(entry.getName()).setPageSize(1000).build();
			DataCatalogClient.ListTagsPagedResponse response = client.listTags(request);

			for (Tag tag : response.iterateAll()) {
				if (tag.getTemplateDisplayName().equals(CTLG_TAG_TMPL_VIEW_V2)) {
					for (Map.Entry<String, TagField> field : tag.getFieldsMap().entrySet()) {
						String key = field.getKey();
						Object value = getTagFieldValue(field);
						viewMap.put(key, value);
						log.debug(logCtlgLookupViewV2Debug, parentViewFQN, key, value);
					}
				}
			}
			log.debug(logCtlgLookupViewV2Finish, parentViewFQN, viewMap);
		} catch (Exception ex) {
			log.debug(logCtlgLookupViewV2Fail, parentViewFQN, viewMap, ex.toString());
			throw new RuntimeException(ex);
		}
		return viewMap;
	}

	public static Map<String, Object> lookupColumnMetadata(String prntProject, String prntDataset, String prntView) {

		String parentViewFQN = String.format("%s.%s.%s", prntProject, prntDataset, prntView);
		log.debug(logCtlgLookupColumnStart, parentViewFQN);

		Map<String, Object> metadataMap = new HashMap<>();
		Map<String, Map<String, Object>> colwiseMetadataMap = new HashMap<>();
		List<Map<String, Object>> catwiseDataDomainCsvList = new ArrayList<>();
		// initialize a map to store category-wise data_domain attributes
		Map<String, List<String>> catwiseDataDomainMap = new HashMap<>();
		for (Category category : Category.values()) {
			catwiseDataDomainMap.put(category.name(), new ArrayList<String>());
		}
		try (DataCatalogClient client = CatalogClient.create()) {
			String linkedResource = String.format(CTLG_LOOKUP_VIEW, prntProject, prntDataset, prntView);
			LookupEntryRequest lookupRequest = LookupEntryRequest.newBuilder().setLinkedResource(linkedResource)
					.build();
			Entry entry = client.lookupEntry(lookupRequest);
			ListTagsRequest request = ListTagsRequest.newBuilder().setParent(entry.getName()).setPageSize(1000).build();
			DataCatalogClient.ListTagsPagedResponse response = client.listTags(request);
			for (Tag tag : response.iterateAll()) {
				String tagName = tag.getTemplateDisplayName();
				if (tagName.equals(CTLG_TAG_TMPL_COLUMN)) {
					Map<String, TagField> columnTag = tag.getFieldsMap();
					String columnName = tag.getColumn();
					String dataDomain = null;
					String riskCategory = columnTag.get(MD_RSK_CTGRY).getEnumValue().getDisplayName();
					if (riskCategory != null && !"".equals(riskCategory.trim())) {
						Category category = Category.lookupByValue(riskCategory.trim());
						if (category != null) {
							String riskCatKey = category.name();
							dataDomain = trim(columnTag.get(MD_DT_DMN).getEnumValue().getDisplayName());
							if (dataDomain != null && !"".equals(dataDomain)
									&& catwiseDataDomainMap.containsKey(riskCatKey)) {
								catwiseDataDomainMap.get(riskCatKey).add(dataDomain);
							}
						}
					}
					log.debug(logCtlgLookupColumnDebug, parentViewFQN, tagName, columnName, riskCategory, dataDomain);
					if (colwiseMetadataMap.get(columnName) == null) {
						colwiseMetadataMap.put(columnName, new HashMap<String, Object>());
					}
					for (Map.Entry<String, TagField> field : tag.getFieldsMap().entrySet()) {
						String key = field.getKey();
						Object value = getTagFieldValue(field);
						colwiseMetadataMap.get(columnName).put(key, String.valueOf(value));
					}
				}
			}
			for (Category category : Category.values()) {
				String dataDomainsCsv = parseAsCsv(catwiseDataDomainMap.get(category.name()));
				if (dataDomainsCsv != null && !"".equals(dataDomainsCsv)) {
					Map<String, Object> catwiseDataDomainCsvMap = new TreeMap<>();
					catwiseDataDomainCsvMap.put(CatalogConstants.MD_RSK_CTGRY, category.getCategory());
					catwiseDataDomainCsvMap.put(CatalogConstants.MD_DT_DMN, dataDomainsCsv);
					catwiseDataDomainCsvList.add(catwiseDataDomainCsvMap);
				}
			}
			metadataMap.put("colwise", colwiseMetadataMap);
			metadataMap.put("catwise", catwiseDataDomainCsvList);
			log.debug(logCtlgLookupColumnFinish1, parentViewFQN, colwiseMetadataMap);
			log.debug(logCtlgLookupColumnFinish2, parentViewFQN, catwiseDataDomainCsvList);
		} catch (Exception ex) {
			log.debug(logCtlgLookupColumnFail, parentViewFQN, catwiseDataDomainCsvList, ex.toString());
			throw new RuntimeException(ex);
		}
		return metadataMap;
	}

	public static Map<String, Object> lookupColumnMetadataV2(String prntProject, String prntDataset, String prntView) {

		String parentViewFQN = String.format("%s.%s.%s", prntProject, prntDataset, prntView);
		log.debug(logCtlgLookupColumnV2Start, parentViewFQN);

		Map<String, Object> metadataMap = new HashMap<>();
		Map<String, Map<String, Object>> colwiseMetadataMap = new HashMap<>();
		List<Map<String, Object>> catwiseDataDomainCsvList = new ArrayList<>();
		// initialize a map to store category-wise data_domain attributes
		Map<String, List<String>> catwiseDataDomainMap = new HashMap<>();
		for (Category category : Category.values()) {
			catwiseDataDomainMap.put(category.name(), new ArrayList<String>());
		}
		try (DataCatalogClient client = CatalogClient.create()) {
			String linkedResource = String.format(CTLG_LOOKUP_VIEW, prntProject, prntDataset, prntView);
			LookupEntryRequest lookupRequest = LookupEntryRequest.newBuilder().setLinkedResource(linkedResource)
					.build();
			Entry entry = client.lookupEntry(lookupRequest);
			ListTagsRequest request = ListTagsRequest.newBuilder().setParent(entry.getName()).setPageSize(1000).build();
			DataCatalogClient.ListTagsPagedResponse response = client.listTags(request);
			for (Tag tag : response.iterateAll()) {
				String tagName = tag.getTemplateDisplayName();
				if (tagName.equals(CTLG_TAG_TMPL_COLUMN_V2)) {
					Map<String, TagField> columnTag = tag.getFieldsMap();
					String columnName = tag.getColumn();
					String dataDomain = null;
					String riskCategory = columnTag.get(MD_RSK_CTGRY).getStringValue();
					if (riskCategory != null && !"".equals(riskCategory.trim())) {
						Category category = Category.lookupByValue(riskCategory.trim());
						if (category != null) {
							String riskCatKey = category.name();
							dataDomain = trim(columnTag.get(MD_DT_DMN).getEnumValue().getDisplayName());
							if (dataDomain != null && !"".equals(dataDomain)
									&& catwiseDataDomainMap.containsKey(riskCatKey)) {
								catwiseDataDomainMap.get(riskCatKey).add(dataDomain);
							}
						}
					}
					log.debug(logCtlgLookupColumnV2Debug, parentViewFQN, tagName, columnName, riskCategory, dataDomain);
					if (colwiseMetadataMap.get(columnName) == null) {
						colwiseMetadataMap.put(columnName, new HashMap<String, Object>());
					}
					for (Map.Entry<String, TagField> field : tag.getFieldsMap().entrySet()) {
						String key = field.getKey();
						Object value = getTagFieldValue(field);
						colwiseMetadataMap.get(columnName).put(key, String.valueOf(value));
					}
				}
			}
			for (Category category : Category.values()) {
				String dataDomainsCsv = parseAsCsv(catwiseDataDomainMap.get(category.name()));
				if (dataDomainsCsv != null && !"".equals(dataDomainsCsv)) {
					Map<String, Object> catwiseDataDomainCsvMap = new TreeMap<>();
					catwiseDataDomainCsvMap.put(CatalogConstants.MD_RSK_CTGRY, category.getCategory());
					catwiseDataDomainCsvMap.put(CatalogConstants.MD_DT_DMN, dataDomainsCsv);
					catwiseDataDomainCsvList.add(catwiseDataDomainCsvMap);
				}
			}
			metadataMap.put("colwise", colwiseMetadataMap);
			metadataMap.put("catwise", catwiseDataDomainCsvList);
			log.debug(logCtlgLookupColumnV2Finish1, parentViewFQN, colwiseMetadataMap);
			log.debug(logCtlgLookupColumnV2Finish2, parentViewFQN, catwiseDataDomainCsvList);
		} catch (Exception ex) {
			log.debug(logCtlgLookupColumnV2Fail, parentViewFQN, catwiseDataDomainCsvList, ex.toString());
			throw new RuntimeException(ex);
		}
		return metadataMap;
	}

	private static Object getTagFieldValue(Map.Entry<String, TagField> field) {
		TagField tagField = field.getValue();
		Object value = null;
		if (tagField.hasBoolValue()) {
			value = String.valueOf(tagField.getBoolValue());
		}
		if (tagField.hasDoubleValue()) {
			value = tagField.getDoubleValue();
		}
		if (tagField.hasEnumValue()) {
			value = tagField.getEnumValue().getDisplayName();
		}
		if (tagField.hasRichtextValue()) {
			value = tagField.getRichtextValue();
		}
		if (tagField.hasStringValue()) {
			value = tagField.getStringValue();
		}
		if (tagField.hasTimestampValue()) {
			value = tagField.getTimestampValue();
		}
		return value;
	}

	private static String trim(final String str) {
		return str == null ? null : str.trim();
	}

	private static String parseAsCsv(List<String> dataDomainList) {
		String dataDomainCsv = dataDomainList.stream().filter(s -> s != null && !"".equals(s)).distinct()
				.collect(joining(","));
		return dataDomainCsv;
	}

}
