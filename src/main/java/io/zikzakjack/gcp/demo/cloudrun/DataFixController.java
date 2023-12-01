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
import io.zikzakjack.gcp.commons.Utils;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class DataFixController {

	@Autowired
	private Environment env;

	@PostConstruct
	public void postConstruct() {
		log.info("[DataOps] DataFixController.postConstruct() Started executing...");
		log.info("[DataOps] DataFixController.postConstruct() Finished executing...");
	}

	@PostMapping("/dataFix")
	public String dataFix(@RequestBody String dataFixFilePath) {
		log.info("[DataOps] DataFixController.dataFix() Started executing...");

		String myProject = getConfig("my.project");
		String myDataset = getConfig("my.dataset");
		dataFixFilePath = dataFixFilePath.trim();

		String dataFixTs = LocalDateTime.now(ZoneOffset.UTC).toString();
		String dataFixQuery = getSql(dataFixFilePath);

		StringBuffer responseBuf = new StringBuffer();
		responseBuf.append("\n dataFixTs >> " + dataFixTs);
		responseBuf.append("\n dataFixFilePath >> " + dataFixFilePath);
		responseBuf.append("\n dataFixQuery >> " + dataFixQuery);

		log.info("dataFixTs >> " + dataFixTs);
		log.info("dataFixFilePath >> " + dataFixFilePath);
		log.info("dataFixQuery >> " + dataFixQuery);

		try {
			BigQueryService.execute(myProject, myDataset, dataFixQuery);
			responseBuf.append("\n dataFix >> succeeded");
			log.info("dataFix  >> succeeded");
		} catch (Exception ex) {
			log.error("[DataOps] datafix() failed ... {}", ex.toString());
			responseBuf.append("\n dataFix >> failed");
		}

		log.info("[DataOps] DataFixController.dataFix() Finished executing...");
		return responseBuf.toString();
	}

	private String getConfig(String configKey) {
		return env.getProperty(configKey);
	}

	private String getSql(String path) {
		return Utils.getSql(path);
	}

}