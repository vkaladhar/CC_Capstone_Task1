import java.util.Comparator;



public class AirportDtls implements Comparable<AirportDtls> {
	

		private String origDest;
		private int cnt;
		
		public AirportDtls(String origDest, int cnt){
			this.origDest = origDest;
			this.cnt = cnt;
		}
		
		public String getOrigDest() {
			return origDest;
		}
		public void setOrigDest(String origDest) {
			this.origDest = origDest;
		}
		public int getCnt() {
			return cnt;
		}
		public void setCnt(int cnt) {
			this.cnt = cnt;
		}
		
		public int compareTo(AirportDtls compareAirportDtls) {
			
			int compareCnt = ((AirportDtls) compareAirportDtls).getCnt(); 
			
			//ascending order
			return compareCnt-this.cnt;
			
			//descending order
			//return compareQuantity - this.quantity;
			
		}	
		
		 public static class Comparators {

		        public static Comparator<AirportDtls> CNT = new Comparator<AirportDtls>() {
		            @Override
		            public int compare(AirportDtls o1, AirportDtls o2) {
		                return o1.compareTo(o2);
		            }
		        };
		 }
		
		@Override
		public String toString() {
			return "AirportDtls [origDest=" + origDest + ", cnt=" + cnt + "]";
		}
		
	}