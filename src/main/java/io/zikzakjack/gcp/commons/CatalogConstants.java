package io.zikzakjack.gcp.commons;

public class CatalogConstants {

	// CATALOG RESOURCES
	public static String CTLG_LOOKUP_PROJECT = "projects/%s/locations/us-central1/entryGroups/%s/entries/%s";
	public static String CTLG_LOOKUP_DATASET = "//bigquery.googleapis.com/projects/%s/datasets/%s";
	public static String CTLG_LOOKUP_VIEW = "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s";
	public static String CTLG_TAG_TMPL_PROJECT = "Project_Template";
	public static String CTLG_TAG_TMPL_PROJECT_V2 = "project_template_v2";
	public static String CTLG_TAG_TMPL_DATASET = "dataset_template";
	public static String CTLG_TAG_TMPL_VIEW = "View_Template";
	public static String CTLG_TAG_TMPL_VIEW_V2 = "View_Template_V2";
	public static String CTLG_TAG_TMPL_COLUMN = "Column_Template";
	public static String CTLG_TAG_TMPL_COLUMN_V2 = "Column_Template_V2";

	// PROJECT METADATA
	public static final String MD_AVL_ST = "available_state";
	public static final String MD_DMN = "domain";
	public static final String MD_DT_ST = "data_state";
	public static final String MD_DT_ST_CHNG_DT = "data_state_change_date";
	public static final String MD_DT_ST_EXPLN = "data_state_explanation";
	public static final String MD_PRJ_TYPE = "project_type";
	public static final String MD_SKL_TM = "skill_team";
	public static final String MD_SRC = "source";

	// VIEW METADATA
	public static final String MD_AVL_CNTRS = "available_countries";
	public static final String MD_AVL_DT_DMN = "available_data_domains";
	public static final String MD_CNTRY_CLMN = "country_column";
	public static final String MD_CNTRY_ELGBLTY = "country_eligibility";
	public static final String MD_MX_RSK = "max_risk";

	// VIEW METADATA V2
	public static final String MD_MX_RSK_CTGRY = "max_risk_category";

	// COLUMN METADATA
	public static final String MD_DT_DMN = "data_domain";
	public static final String MD_RSK_CTGRY = "risk_category";

	// DATASET METADATA
	public static final String MD_ACS_NVGTN_GRP = "access_navigation_group";
	public static final String MD_AVL_FOR_ENDUSER_PRVSNG = "available_for_enduser_provisioning";
	public static final String MD_DATASET_TYPE = "dataset_type";
	public static final String MD_DS_FRNDLY_NAME = "ds_friendly_name";

}
