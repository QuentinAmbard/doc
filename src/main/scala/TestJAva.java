import java.io.Serializable;

public class TestJAva {

    private String text;
    private String text1;
    private String text2;
    private TestJAva2 test;

    public TestJAva(String text, String text1, String text2, TestJAva2 test) {
        this.text = text;
        this.text1 = text1;
        this.text2 = text2;
        this.test = test;
    }

    public TestJAva2 getTest() {
        return test;
    }

    public void setTest(TestJAva2 test) {
        this.test = test;
    }

    public String getText() {
        return text;

    }

    public void setText(String text) {
        this.text = text;
    }

    public String getText1() {
        return text1;
    }

    public void setText1(String text1) {
        this.text1 = text1;
    }

    public String getText2() {
        return text2;
    }

    public void setText2(String text2) {
        this.text2 = text2;
    }
}
