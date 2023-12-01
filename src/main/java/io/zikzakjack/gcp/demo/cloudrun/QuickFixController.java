package io.zikzakjack.gcp.demo.cloudrun;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.zikzakjack.gcp.commons.BigQueryService;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class QuickFixController {

	@Autowired
	private Environment env;

	@PostConstruct
	public void postConstruct() {
		log.info("[DataOps] QuickFixController.postConstruct() Started executing...");
		log.info("[DataOps] QuickFixController.postConstruct() Finished executing...");
	}

	@PostMapping("/quickFix")
	public String quickFix(@RequestBody String quickFixQuery) {
		log.info("[DataOps] QuickFixController.quickFix() Started executing...");

		String myProject = getConfig("my.project");
		String myDataset = getConfig("my.dataset");

		String quickFixTs = LocalDateTime.now(ZoneOffset.UTC).toString();
		quickFixQuery = quickFixQuery.replaceAll("\n", "\t");
		quickFixQuery = quickFixQuery.replaceAll("\r", "\t");

		StringBuffer responseBuf = new StringBuffer();
		responseBuf.append("\n quickFixTs >> " + quickFixTs);
		responseBuf.append("\n quickFixQuery >> " + quickFixQuery);

		log.info("quickFixTs >> " + quickFixTs);
		log.info("quickFixQuery >> " + quickFixQuery);

		try {
			BigQueryService.execute(myProject, myDataset, quickFixQuery);
			responseBuf.append("\n quickFix >> succeeded");
			log.info("quickFix  >> succeeded");
		} catch (Exception ex) {
			log.error("[DataOps] quickFix() failed ... {}", ex.toString());
			responseBuf.append("\n quickFix >> failed");
		}

		log.info("[DataOps] QuickFixController.quickFix() Finished executing...");
		return responseBuf.toString();
	}

	private String getConfig(String configKey) {
		return env.getProperty(configKey);
	}

}