#22.12.19
#1694 Reformat Phone Numbers 额啊虽然是Java但是思路打开
public String reformatNumber(String number) {
        number = number.replace(" ","");
        number = number.replace("-","");
        number = number.replaceAll("(?<=\\G\\d{3})(?!$)", "-");
        number = number.replaceAll("\\b(\\d{2})(\\d+)-(\\d)$", "$1-$2$3");
        return number;

    }