package poly.service.impl.crawling.impl;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Service;

import poly.service.impl.crawling.ICrawlingService;
import poly.util.CmmUtil;

@Service("CrawlingService")
public class CrawlingService implements ICrawlingService {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	/**
	 * 멜론 노래 수집하기
	 */
	@Override
	public Set<String> doColletMelon() throws Exception {

		log.info(this.getClass().getName() + ".doColletMelon Start!");

		// 중복제거하여 데이터 저장함
		Set<String> pSet = new TreeSet<String>();

		// 멜론 Top100 중 50위까지 정보 가져오는 페이지
		String url = "https://www.melon.com/chart/index.htm";

		// JSOUP 라이브러리를 통해 사이트 접속되면, 그 사이트의 전체 HTML소스 저장할 변수
		Document doc = null; //

		doc = Jsoup.connect(url).get();

		// <div class="service_list_song"> 이 태그 내에서 있는 HTML소스만 element에 저장됨
		Elements element = doc.select("div.service_list_song");

		// Iterator을 사용하여 멜론차트 정보를 가져오기
		Iterator<Element> songList = element.select("div.wrap_song_info").iterator(); // 멜론 50위까지 차크

		while (songList.hasNext()) {

			Element songInfo = songList.next();

			// 크롤링을 통해 데이터 저장하기
			String song = CmmUtil.nvl(songInfo.select("div.ellipsis.rank01 a").text()); // 노래
			String singer = CmmUtil.nvl(songInfo.select("div.ellipsis.rank02 a").eq(0).text()); // 가수

			// 3글자 가수는 제외하고 수집함
			if ((song.length() > 0) && (singer.length() > 0)) {

//				log.info("singer : " + singer);

				singer = singer.toUpperCase().replaceAll("\\s", ""); // 가수
				int idx = singer.indexOf("(");

				if (idx > 0) {
					singer = singer.substring(0, idx);

				}

				singer = singer.replaceAll("[^a-z|A-Z|가-힣|0-9]", ""); // 가수

//				log.info("singer_new : " + singer);

				if (!(singer.length() == 1 || singer.length() == 3)) {
					pSet.add(singer); // 노래 저장하기

				}

			}

			songInfo = null;
		}

		element = null;
		doc = null;

		log.info(this.getClass().getName() + ".doColletMelon End!");

		return pSet;
	}

	@Override
	public Set<String> doColletMovie() throws Exception {

		log.info(this.getClass().getName() + ".doColletMovie Start!");

		// 중복제거하여 데이터 저장함
		Set<String> pSet = new TreeSet<String>();

		// CGV 영화 순위 정보 가져올 사이트 주소
		String url = "http://www.cgv.co.kr/movies/";

		// JSOUP 라이브러리를 통해 사이트 접속되면, 그 사이트의 전체 HTML소스 저장할 변수
		Document doc = null; //

		// 사이트 접속(http프로토롱만 가능, https 프로토콜은 보안상 안됨)
		doc = Jsoup.connect(url).get();

		// CGV 웹페이지의 전체 소소 중 일부 태그를 선택하기 위해 사용
		// <div class="sect-movie-chart"> 이 태그 내에서 있는 HTML소스만 element에 저장됨
		Elements element = doc.select("div.sect-movie-chart");

		// Iterator을 사용하여 영화 순위 정보를 가져오기
		Iterator<Element> movie_name = element.select("strong.title").iterator(); // 영화 이름

		// 수집된 데이터 DB에 저장하기
		while (movie_name.hasNext()) {
			String nm = CmmUtil.nvl((String) movie_name.next().text());
			nm = nm.toUpperCase().replaceAll("\\s", "");
			nm = nm.replaceAll("-", "_");

			int stIdx = nm.indexOf("(");
			int endIdx = nm.indexOf(")");

			if (stIdx > -1 && endIdx > 0) {
				String replaceWord = nm.substring(stIdx, endIdx + 1);
				log.info("replaceWord : " + replaceWord);

				nm = nm.replace(replaceWord, "");
				nm = nm.replaceAll("\\s", "");

				log.info("replaceWord new : " + nm);

			}

//			log.info("nm : " + nm);

			pSet.add(nm); // 노래 저장하기

		}

		movie_name = null;
		element = null;
		doc = null;

		log.info(this.getClass().getName() + ".doColletMovie End!");

		return pSet;
	}

	@Override
	public Set<String> doColletTV1() throws Exception {

		log.info(this.getClass().getName() + ".doColletTV1 Start!");

		// 중복제거하여 데이터 저장함
		Set<String> pSet = new TreeSet<String>();

		// 드라마 시청률 정보 가져오는 페이지
		String url = "https://www.nielsenkorea.co.kr/tv_terrestrial_day.asp?menu=Tit_1&sub_menu=1_2&area=00";

		// JSOUP 라이브러리를 통해 사이트 접속되면, 그 사이트의 전체 HTML소스 저장할 변수
		Document doc = null;

		doc = Jsoup.connect(url).get();

		// <div class="service_list_song"> 이 태그 내에서 있는 HTML소스만 element에 저장됨
		Elements element = doc.select("table.ranking_tb");
		// Iterator을 사용하여 드라마 시청률 순위 정보를 가져오기
		int loopCnt = element.select("td.tb_txt").size() / 2;

		Iterator<Element> dramaIt = element.select("td.tb_txt").iterator(); // 닐슨코리아 드라마 시청률 순위

		while (dramaIt.hasNext() && loopCnt-- > 0) {

			Element dramaInfo = dramaIt.next();
			// 크롤링을 통해 데이터 저장하기
			String drama = CmmUtil.nvl(dramaInfo.select("td.tb_txt").eq(0).text()); // 드라마

			if (drama.length() > 0 && (drama.indexOf("뉴스") < 0)) {

				drama = drama.toUpperCase().replaceAll("\\s", ""); // 드라마

				int stIdx = drama.indexOf("(");
				int endIdx = drama.indexOf(")");

				if (stIdx > 0 && (stIdx < endIdx)) {
					drama = drama.substring(stIdx, endIdx);

				}

				drama = drama.replaceAll("[0-9]{1,2}부", ""); // 1~99부 제거
				drama = drama.replaceAll("[^a-z|A-Z|가-힣|0-9]", ""); // 드라마

				log.info("drama : " + drama);

				if (drama.length() > 0) {
					pSet.add(drama); // 드라마 저장하기

				}

			}

			dramaInfo = null;
		}

		dramaIt = null;
		element = null;
		doc = null;

		log.info(this.getClass().getName() + ".doColletTV1 End!");

		return pSet;
	}

	@Override
	public Set<String> doColletTV2() throws Exception {

		log.info(this.getClass().getName() + ".doColletTV2 Start!");

		// 중복제거하여 데이터 저장함
		Set<String> pSet = new TreeSet<String>();

		// 드라마 시청률 정보 가져오는 페이지
		String url = "https://www.nielsenkorea.co.kr/tv_terrestrial_day.asp?menu=Tit_1&sub_menu=2_2&area=00";

		// JSOUP 라이브러리를 통해 사이트 접속되면, 그 사이트의 전체 HTML소스 저장할 변수
		Document doc = null;

		doc = Jsoup.connect(url).get();

		// <div class="service_list_song"> 이 태그 내에서 있는 HTML소스만 element에 저장됨
		Elements element = doc.select("table.ranking_tb");
		// Iterator을 사용하여 드라마 시청률 순위 정보를 가져오기
		int loopCnt = element.select("td.tb_txt").size() / 2;

		Iterator<Element> dramaIt = element.select("td.tb_txt").iterator(); // 닐슨코리아 드라마 시청률 순위

		while (dramaIt.hasNext() && loopCnt-- > 0) {

			Element dramaInfo = dramaIt.next();
			// 크롤링을 통해 데이터 저장하기
			String drama = CmmUtil.nvl(dramaInfo.select("td.tb_txt").eq(0).text()); // 드라마

			if (drama.length() > 0 && (drama.indexOf("뉴스") < 0)) {

				drama = drama.toUpperCase().replaceAll("\\s", ""); // 드라마

				int stIdx = drama.indexOf("(");
				int endIdx = drama.indexOf(")");

				if (stIdx > 0 && (stIdx < endIdx)) {
					drama = drama.substring(stIdx, endIdx);

				}

				drama = drama.replaceAll("[0-9]{1,2}부", ""); // 1~99부 제거
				drama = drama.replaceAll("[^a-z|A-Z|가-힣|0-9]", ""); // 드라마

				log.info("drama : " + drama);

				if (drama.length() > 0) {
					pSet.add(drama); // 드라마 저장하기

				}

			}

			dramaInfo = null;
		}

		dramaIt = null;
		element = null;
		doc = null;

		log.info(this.getClass().getName() + ".doColletTV2 End!");

		return pSet;
	}

	@Override
	public Set<String> doColletTV3() throws Exception {

		log.info(this.getClass().getName() + ".doColletTV3 Start!");

		// 중복제거하여 데이터 저장함
		Set<String> pSet = new TreeSet<String>();

		// 드라마 시청률 정보 가져오는 페이지
		String url = "https://www.nielsenkorea.co.kr/tv_terrestrial_day.asp?menu=Tit_1&sub_menu=3_2&area=00";

		// JSOUP 라이브러리를 통해 사이트 접속되면, 그 사이트의 전체 HTML소스 저장할 변수
		Document doc = null;

		doc = Jsoup.connect(url).get();

		// <div class="service_list_song"> 이 태그 내에서 있는 HTML소스만 element에 저장됨
		Elements element = doc.select("table.ranking_tb");
		// Iterator을 사용하여 드라마 시청률 순위 정보를 가져오기
		int loopCnt = element.select("td.tb_txt").size() / 2;

		Iterator<Element> dramaIt = element.select("td.tb_txt").iterator(); // 닐슨코리아 드라마 시청률 순위

		while (dramaIt.hasNext() && loopCnt-- > 0) {

			Element dramaInfo = dramaIt.next();
			// 크롤링을 통해 데이터 저장하기
			String drama = CmmUtil.nvl(dramaInfo.select("td.tb_txt").eq(0).text()); // 드라마

			if (drama.length() > 0 && (drama.indexOf("뉴스") < 0)) {

				drama = drama.toUpperCase().replaceAll("\\s", ""); // 드라마

				int stIdx = drama.indexOf("(");
				int endIdx = drama.indexOf(")");

				if (stIdx > 0 && (stIdx < endIdx)) {
					drama = drama.substring(stIdx, endIdx);

				}

				drama = drama.replaceAll("[0-9]{1,2}부", ""); // 1~99부 제거
				drama = drama.replaceAll("[^a-z|A-Z|가-힣|0-9]", ""); // 드라마

				log.info("drama : " + drama);

				if (drama.length() > 0) {
					pSet.add(drama); // 드라마 저장하기

				}

			}

			dramaInfo = null;
		}

		dramaIt = null;
		element = null;
		doc = null;

		log.info(this.getClass().getName() + ".doColletTV3 End!");

		return pSet;
	}

}
