package gash.router.persistence;

public class ReplicationInfo {

	private String fileName;
	private byte[] fileContent;
	private int chunkOrder;

	public ReplicationInfo() {

	}
	
	public ReplicationInfo(String fileName, byte[] fileContent, int chunkOrder) {
		super();
		this.fileName = fileName;
		this.fileContent = fileContent;
		this.chunkOrder = chunkOrder;
	}




	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public byte[] getFileContent() {
		return fileContent;
	}

	public void setFileContent(byte[] fileContent) {
		this.fileContent = fileContent;
	}

	public int getChunkOrder() {
		return chunkOrder;
	}

	public void setChunkOrder(int chunkOrder) {
		this.chunkOrder = chunkOrder;
	}

}
