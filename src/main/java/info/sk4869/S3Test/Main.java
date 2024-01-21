package info.sk4869.S3Test;

public class Main {

	public static void main(String[] args) {
		final S3Access s3Access = new S3Access();

		if (args.length == 0) {
			System.out.println("バケット名を入力してください。");
			return;
		}

		try {
			s3Access.show(args[0]);
		} catch (Exception e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
	}

}