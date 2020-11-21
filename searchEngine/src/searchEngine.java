
import java.io.*;
import javax.swing.*;
import java.awt.event.ActionListener;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.io.FileInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set; 
import java.util.List;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.dataproc.v1.JobMetadata;
import com.google.cloud.ReadChannel;
import com.google.cloud.dataproc.v1.HadoopJob;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.api.gax.paging.Page;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.services.storage.model.Bucket;
import com.google.cloud.storage.CopyWriter;


public class searchEngine extends JFrame implements ActionListener{
    private JFrame frame; 
    private JPanel contentPane;
    private JButton chooseFile;
    private JButton constructIndices;
    private JButton loadEngine;
    private JButton searchTerm;
    private JButton searchT;
    private JButton topN;
    private JButton searchN;
    private JLabel label;
    private JLabel anotherLabel;
    private JTextArea text;
    private JTextField termToSearch;
    private JTextField numForTopN;
    private JButton backButton;
    private JTable tableForN;
    private JTable tableForSearch;
    private JScrollPane sp;
    private JTextArea fileNameDisplay;
    
    private File[] files; 
    private String currentJob;
    private String bucketName;
    private String projectId;

    private List<Entry<String, Integer>> topNList;
    private List<Entry<String, Integer>> searchedTermList;
    
    public searchEngine() {
        frame = new JFrame("My Search Engine");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        contentPane = new JPanel();
        contentPane.setLayout(new GridBagLayout());
        
        GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;
        
        label = new JLabel("Load My Engine", SwingConstants.CENTER);
        text = new JTextArea();

        chooseFile = new JButton("Choose Files");
        constructIndices = new JButton("Construct Inverted Indices");
        loadEngine = new JButton("Load Engine");
        searchTerm = new JButton("Search for Term");
        searchT = new JButton("Search");
        topN = new JButton("Top-N");
        searchN = new JButton("Search");
        termToSearch = new JTextField();
        termToSearch.setPreferredSize( new Dimension( 200, 24 ) );
        numForTopN = new JTextField();
        numForTopN.setPreferredSize( new Dimension( 200, 24 ) );
        anotherLabel= new JLabel("Please Select Action");
        backButton = new JButton("Go back to search");
        fileNameDisplay = new JTextArea("");
        
        chooseFile.addActionListener(this);
        constructIndices.addActionListener(this);
        loadEngine.addActionListener(this);
        searchTerm.addActionListener(this);
        searchT.addActionListener(this);
        topN.addActionListener(this);
        searchN.addActionListener(this);
        termToSearch.addActionListener(this);
        numForTopN.addActionListener(this);
        backButton.addActionListener(this);
        
        addLabel();
        addChooseFileButton();
        addConstructIndicesButton();
        
        bucketName = "dataproc-staging-us-east1-469710197436-tqybseji";
    	projectId = "cs1660-293317";
      
        frame.add(contentPane);
        frame.setSize(700, 700);
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }
    public void actionPerformed(ActionEvent e){
        Object buttonPressed = e.getSource();
        
        if (buttonPressed == chooseFile) {
            chooseFile();
            for(File file : files)
		    {
				fileNameDisplay.append(file.getName()+"\n");
		    }
            addFilenameDisplay();
            contentPane.remove(constructIndices);
            addLoadEngineButton();
            contentPane.revalidate();
        	frame.revalidate();
        } else if (buttonPressed == loadEngine) {
        	currentJob = UUID.randomUUID().toString();
            if (files != null) {
            	for (File file: files) {
            		String objectName = file.getName();
            		String path = file.getPath().replace('\\', '/');
            		try {
						uploadFile(objectName, path, "invertedIndices");;
					} catch (IOException e1) {
						System.err.println("Upload file: " + objectName + " is unsuccessful");
					}
            	}
            	try {
            		boolean success = submitJob("invertedIndices");
            		if (success) {
            			engineLoadedPage();
            		}
            		else {
            			text.setText("Engine failed");
            		}
            	} catch(Exception ex) {
            		System.err.println("Problems submitting job");
            	}
            } else {
                text.append("No files chosen!");
            } 
        } else if (buttonPressed == searchTerm) {
        	searchTermPage();
        } else if (buttonPressed == topN) {
        	topNPage();
        } else if (buttonPressed == searchT) {
        	String input = termToSearch.getText();
        	searchTermResultsPage(input);
        } else if (buttonPressed == searchN) {
        	int num = Integer.parseInt(numForTopN.getText());
        	try {
        		if (topNList == null || topNList.isEmpty()) {
        			submitJob("topN");
        		}
        		topNResultsPage(num);
        	} catch (Exception ex) {
        		System.err.println(ex.getMessage());
        	}
        } else if (buttonPressed == backButton) {
        	engineLoadedPage();
        }
        
    }
    public void chooseFile() {
    	fileNameDisplay.selectAll();
		fileNameDisplay.replaceSelection("");
        JFileChooser fc = new JFileChooser();
        fc.setMultiSelectionEnabled(true); 
        int returnVal = fc.showSaveDialog(null);    
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            files = fc.getSelectedFiles();
            for (File file : files) {
                System.out.println("Opening: " + file.getName() + "." );
                text.append(file.getName() + "\n");
            }
        } else {
            System.out.println("Open command cancelled by user." );
        }
    }
    
    public void uploadFile(String objectName, String filePath, String jobName) throws IOException {
    	Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        BlobId blobId = BlobId.of(bucketName, "input-" + jobName + "-" + currentJob + "/" + objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));

        System.out.println(
            "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName + "\n");
      }
    
    public boolean submitJob(String jobName) throws IOException{
    	/*
    	 * Code is taken from DataProc documentation 
    	 * (https://cloud.google.com/dataproc/docs/guides/submit-job)
    	 */
    	String cluster = "cluster-e42e";
		String region = "us-east1";
		String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);
		String mainClass = jobName;
		
		String input = "gs://dataproc-staging-us-east1-469710197436-tqybseji/input-" + jobName + "-" + currentJob;
		if (jobName.equals("topN")) {
			input = "gs://dataproc-staging-us-east1-469710197436-tqybseji/output-invertedIndices-" + currentJob;
		}
		
		try {
			JobControllerSettings jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
			JobControllerClient jobControllerClient = JobControllerClient.create(jobControllerSettings);
			
			JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(cluster).build();
			HadoopJob myJob = HadoopJob.newBuilder().setMainClass(mainClass)
					.setMainClass(mainClass)
					.addJarFileUris("gs://dataproc-staging-us-east1-469710197436-tqybseji/" + jobName +".jar")
					.addArgs(input)
					.addArgs("gs://dataproc-staging-us-east1-469710197436-tqybseji/output-" + jobName + "-" + currentJob)
					.build();

			Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(myJob).build();
			
			OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
			          jobControllerClient.submitJobAsOperationAsync(projectId, region, job);
			
			System.out.println("Job submitted for " + jobName);
			
			try {
				Job response = submitJobAsOperationAsyncRequest.get(10, TimeUnit.MINUTES);
							
				switch(response.getStatus().getState()) {
					case DONE:
						System.out.println("Job successfully completed\n");
						return true;
					case CANCELLED:
						System.out.println("Job was cancelled");
						break;
					case ERROR:
						System.out.println("An error occurred with job");
						break;
					default: 
						break;
				}
			} catch(TimeoutException e) {
				System.err.println(e.getMessage());
			}
	
		} catch(Exception e) {
			System.err.println(e.getMessage());
		}
		
		return false;
    }
    
    public void getOutput(String jobName, String wordToSearch) {
    	Map<String, Integer> map = new HashMap<>();
    	
    	String directoryPrefix = "output-" + jobName + "-" + currentJob + "/part-r-";
    	Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        com.google.cloud.storage.Bucket bucket = storage.get(bucketName);
        Page<Blob> blobs =
                bucket.list(Storage.BlobListOption.prefix(directoryPrefix),
                        Storage.BlobListOption.currentDirectory());
        System.out.println("Getting data from Storage\n"); 
        for (Blob blob : blobs.iterateAll()) {
        	byte[] content = blob.getContent();
        	String s = new String(content);
	    	String[] str = s.split("\n");
	    	System.out.println("Bucketname: " + blob.getName());
	    	
	    	if (jobName.equals("topN")) {
		    	for(int i = 0; i < str.length; i++) {
		    		String[] tokens = str[i].split("\t");
		    		map.put(tokens[1], Integer.parseInt(tokens[0]));
		    	}
	    	} else if(jobName.equals("invertedIndices")) {
	    		System.out.println(str.length);
	    		for(int i = 0; i < str.length; i++) {
		    		String[] tokens = str[i].split("[\t:\s]");
		    		if (tokens[0].equals(wordToSearch)) {
		    			System.out.println("Word found");
		    			String key = tokens[0] + ":" + tokens[1];
		    			map.put(key, Integer.parseInt(tokens[2]));
		    			if (tokens.length > 3) {
		    				for (int k = 3; k < tokens.length - 1; k += 2) {
		    					String stri = tokens[0] + ":" +  tokens[k];
		    					map.put(stri, Integer.parseInt(tokens[k+1]));
		    				}
		    			}
		    		}
	    		} 
	    	}
    	}
        System.out.println("Finished retrieving data.\n");
        if (jobName.equals("topN")) {
        	map.remove("the");
        	topNList = entriesSortedByValues(map);
        } else if (jobName.equals("invertedIndices")) {
        	searchedTermList = entriesSortedByValues(map);
        }
    }
    
    public void createTable(String jobName, int row, int column) {
    	String[][] data = createArray(jobName, row, column);
    	if (jobName.equals("topN")) {
    		String[] columnNames = {"Term","Total Frequency"}; 
    		tableForN = new JTable(data, columnNames);
    		tableForN.setBounds(20, 20, 300, 500);
    		sp = new JScrollPane(tableForN);
    	} else if (jobName.equals("invertedIndices")) {
    		String[] columnNames = {"Doc ID","Doc Folder", "Doc Name", "Frequencies"}; 
    		tableForSearch = new JTable(data, columnNames);
    		tableForSearch.setBounds(20, 20, 700, 500);
    		sp = new JScrollPane(tableForSearch);
    	}
    }
    
    private String[][] createArray(String jobName, int row, int column) {
    	String[][] data = new String[row][column];
    	
    	if (jobName.equals("topN")) {
    		for (int i = 0; i < row; i++) {
    			for (int j = 0; j < column; j++) {
    				data[i][j] = topNList.get(i).getKey();
    				data[i][j+1] = Integer.toString(topNList.get(i).getValue());
    				j++;
    			}
    		}
    	} else if (jobName.equals("invertedIndices")) {
    		for (int i = 0; i < row; i++) {
    			String[] tokens = searchedTermList.get(i).getKey().split(":");
    			for (int j = 0; j < column; j++) {
    				data[i][j++] = Integer.toString(i+1);
    				data[i][j++] = tokens[1];
    				data[i][j++] = tokens[0];
    				data[i][j] = Integer.toString(searchedTermList.get(i).getValue());
    			}
    		}
    	}
    	
    	return data;
    }
    
    /**
     * Code Taken from 
     * https://stackoverflow.com/questions/11647889/sorting-the-mapkey-value-in-descending-order-based-on-the-value
     */
    private static <K,V extends Comparable<? super V>> List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {

		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());
		
		Collections.sort(sortedEntries, new Comparator<Entry<K,V>>() {
	        @Override
	        public int compare(Entry<K,V> e1, Entry<K,V> e2) {
	            return e2.getValue().compareTo(e1.getValue());
	        }
		});
		
		return sortedEntries;
	}
    
    public void addLabel() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 0; 
        contentPane.add(label, gbc);
    }

    public void addChooseFileButton() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 1; 
        contentPane.add(chooseFile, gbc);
    }
    public void addFilenameDisplay() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 2; 
        contentPane.add(fileNameDisplay, gbc);
    }
    
    public void addConstructIndicesButton() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 3; 
        contentPane.add(constructIndices, gbc);
    }
    
    public void addLoadEngineButton() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 3; 
        contentPane.add(loadEngine, gbc);
    }
    
    public void addSearchTermButton() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 3; 
        contentPane.add(searchTerm, gbc);
    }
    
    public void addSearchTButton() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 3; 
        contentPane.add(searchT, gbc);
    }
    
	public void addTopNButton() {
		GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 4; 
        contentPane.add(topN, gbc);
	}
	
	public void addSearchNButton() {
		GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 3; 
        contentPane.add(searchN, gbc);
	}
	
	public void addAnotherLabel() {
		GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 2; 
        contentPane.add(anotherLabel, gbc);
	}
	
	public void addBackButton() {
		GridBagConstraints gbc = new GridBagConstraints();  
        gbc.anchor = GridBagConstraints.NORTHEAST;
        contentPane.add(backButton, gbc);
	}
	
    public void searchTermPage() {
    	contentPane.removeAll();
    	label.setText("Enter Your Search Term");
    	addLabel();
    	
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 2;
        contentPane.add(termToSearch, gbc);
    	addSearchTButton();
    	contentPane.revalidate();
    	contentPane.repaint();
    }
    

    
    public void searchTermResultsPage(String term) {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 4;
    	contentPane.removeAll();
    	label.setText("You searched for the term: " + term);
    	addLabel();
    	
		Instant start = Instant.now();
    	getOutput("invertedIndices", term);
    	Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        
		createTable("invertedIndices", searchedTermList.size(), 4);
		contentPane.add(sp,gbc);
		
		anotherLabel.setText("Your search was executed in " + timeElapsed + "ms");
    	addAnotherLabel();
    	addBackButton();
		contentPane.revalidate();
    	contentPane.repaint();
    	frame.invalidate();
    	frame.validate();
    	frame.repaint();
    }
    
    public void topNPage() {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 2;

    	contentPane.removeAll();
    	
    	label.setText("Enter Your N Value");
    	addLabel();
    	contentPane.add(numForTopN, gbc);
    	addSearchNButton();
    	contentPane.revalidate();
    	contentPane.repaint();
    	frame.invalidate();
    	frame.validate();
    	frame.repaint();
    }
    
    public void topNResultsPage(int num) {
    	GridBagConstraints gbc = new GridBagConstraints();  
        gbc.fill = GridBagConstraints.HORIZONTAL;  
        gbc.gridx = 0;  
        gbc.gridy = 3;
    	contentPane.removeAll();
    	
    	anotherLabel.setText("Top-N Frequent Terms");
    	addAnotherLabel();
    	addBackButton();
    	
    	getOutput("topN", null);
		createTable("topN", num, 2);
		
		contentPane.add(sp,gbc);
    	contentPane.revalidate();
    	contentPane.repaint();
    	frame.invalidate();
    	frame.validate();
    	frame.repaint();
    }
    
    public void engineLoadedPage() {
    	topNList = null;
    	tableForSearch = null;
    	contentPane.removeAll();
		label.setText("Engine was loaded & Inverted indicies were constructed successfully!");
		addLabel();
		anotherLabel.setText("Please Select Action");
		addAnotherLabel();
		addSearchTermButton();
		addTopNButton();
		contentPane.revalidate();
		contentPane.repaint();
		frame.invalidate();
		frame.validate();
		frame.repaint();
    }
    
    public static void main(String args[]) throws IOException{
        searchEngine searchEngine = new searchEngine();
     }

     
}
