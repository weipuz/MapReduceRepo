package org.CMPT732A1;

public class testprint {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String instr = "We’re 1234up a2ll ;;18night to the sun";
	    String outstr = instr.replaceAll("[^a-zA-Z ]", " ");
	    System.out.println(outstr);
	}

}
