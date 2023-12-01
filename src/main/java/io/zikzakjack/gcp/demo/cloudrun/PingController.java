package io.zikzakjack.gcp.demo.cloudrun;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import javax.annotation.PostConstruct;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class PingController {

	@PostConstruct
	public void postConstruct() {
		log.info("[DataOps] PingController.postConstruct() Started executing...");
		log.info("[DataOps] PingController.postConstruct() Finished executing...");
	}

	@GetMapping("/ping")
	public String ping() {
		log.info("[DataOps] PingController.ping() Started executing...");

		String pingTs = LocalDateTime.now(ZoneOffset.UTC).toString();
		String response = "ping  >> " + pingTs;
		log.info(response);

		log.info("[DataOps] PingController.ping() Finished executing...");
		return response;
	}

}