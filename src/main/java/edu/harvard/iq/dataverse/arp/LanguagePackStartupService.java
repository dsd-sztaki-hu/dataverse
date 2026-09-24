package edu.harvard.iq.dataverse.arp;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import jakarta.ejb.Timeout;
import jakarta.ejb.Timer;
import jakarta.ejb.TimerConfig;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Optional startup pull of English/Hungarian packs from GitHub.
 * Failure is logged; Dataverse keeps the image-seeded / volume files.
 */
@Startup
@Singleton
public class LanguagePackStartupService {

    private static final Logger logger = Logger.getLogger(LanguagePackStartupService.class.getCanonicalName());

    private static final long INITIAL_DELAY_MS = 15_000L;

    @Resource
    jakarta.ejb.TimerService timerService;

    @EJB
    LanguagePackUpdateService languagePackUpdateService;

    @PostConstruct
    public void init() {
        if (!languagePackUpdateService.isUpdateOnStartEnabled()) {
            logger.info("Skipping language pack update on start (disabled)");
            return;
        }
        logger.info("Scheduling language pack update on start");
        timerService.createSingleActionTimer(INITIAL_DELAY_MS, new TimerConfig(null, false));
    }

    @Timeout
    public void handleTimeout(Timer timer) {
        try {
            LanguagePackUpdateService.UpdateResult result = languagePackUpdateService.updateFromConfig();
            logger.info("Language pack update on start wrote " + result.writtenFiles.size()
                    + " files from " + result.repoUrl + " @ " + result.ref);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Language pack update on start failed; using existing files", e);
        }
    }
}
