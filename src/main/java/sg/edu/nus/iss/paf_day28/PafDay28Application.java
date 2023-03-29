package sg.edu.nus.iss.paf_day28;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import sg.edu.nus.iss.paf_day28.repositories.TvshowRepository;

@SpringBootApplication
public class PafDay28Application implements CommandLineRunner{

	@Autowired
	TvshowRepository tvshowRepo;

	public static void main(String[] args) {
		SpringApplication.run(PafDay28Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		// tvshowRepo.findTvshowsByType("Reality");
		// tvshowRepo.groupShowsByTimezone();
		// tvshowRepo.summarizeTvShows2("Scripted");
		tvshowRepo.showCategory();

	}


}
