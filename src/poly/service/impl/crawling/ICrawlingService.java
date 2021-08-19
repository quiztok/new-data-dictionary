package poly.service.impl.crawling;

import java.util.Set;

public interface ICrawlingService {

	/**
	 * 멜론가수이름 수집하기
	 */
	public Set<String> doColletMelon() throws Exception;

	/**
	 * CGV영화제목 수집하기
	 */
	public Set<String> doColletMovie() throws Exception;

	/**
	 * 닐슨코리아 지상파 수집하기
	 */
	public Set<String> doColletTV1() throws Exception;

	/**
	 * 닐슨코리아 종합편성 수집하기
	 */
	public Set<String> doColletTV2() throws Exception;

	/**
	 * 닐슨코리아 케이블 수집하기
	 */
	public Set<String> doColletTV3() throws Exception;
}
