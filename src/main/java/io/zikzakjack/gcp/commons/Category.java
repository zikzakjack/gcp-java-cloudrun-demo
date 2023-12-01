package io.zikzakjack.gcp.commons;

import java.util.HashMap;
import java.util.Map;

public enum Category {

	CAT0("category 0"), CAT1("category 1"), CAT2("category 2"), CAT3("category 3");

	static final Map<String, Category> lookupMap = new HashMap<String, Category>();

	Category(String category) {
		if (category != null && !"".equals(category.trim())) {
			this.category = category;
		} else {
			this.category = "null";
		}
	}

	static {
		for (Category category : Category.values()) {
			lookupMap.put(category.getCategory(), category);
		}
	}

	private String category;

	public String getCategory() {
		return category;
	}

	public static final Category lookupByValue(String value) {
		return lookupMap.get(value);
	}

	@Override
	public String toString() {
		return getCategory();
	}

}
