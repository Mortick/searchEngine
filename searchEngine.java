import java.awt.Color;
import java.io.*;
import java.util.*;
import javax.swing.*;
import java.awt.event.ActionListener; 
import java.awt.event.*;
import java.awt.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.cloud.dataproc.v1.HadoopJob;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class searchEngine extends JFrame implements ActionListener{
    JFrame frame; 
    JPanel contentPane;
    JButton chooseFile;
    JButton constructIndices;
    JButton loadEngine;
    JButton searchTerm;
    JButton topN;
    JLabel label;
    JTextArea text;
    File files[];

    public searchEngine() {
        frame = new JFrame("Search Engine");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        contentPane = new JPanel();
        contentPane.setLayout(new GridBagLayout());
        contentPane.setBackground(Color.WHITE);

        label = new JLabel("Load My Engine");
        text = new JTextArea();

        chooseFile = new JButton("Choose Files");
        constructIndices = new JButton("Construct Inverted Indices");
        loadEngine = new JButton("Load Engine");
        searchTerm = new JButton("Search for Term");
        topN = new JButton("Top-N");
        chooseFile.addActionListener(this);
        constructIndices.addActionListener(this);
        
        contentPane.add(label);
        contentPane.add(chooseFile);
        contentPane.add(text);
        contentPane.add(constructIndices);
      
        frame.add(contentPane);
        frame.setSize(1000, 640);
        frame.setVisible(true);
    }
    public void actionPerformed(ActionEvent e) {
        Object buttonPressed = e.getSource();
        
        if (buttonPressed == chooseFile) {
            chooseFile(); 
        } else if (buttonPressed == constructIndices) {
            if (files != null) {
                submitJob();
            } else {
                text.append("No files chosen!");
            }
            
        }
        
    }
    public void chooseFile() {
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

    public void submitJob() throws IOException, InterruptedException{
        String region = "us-east1";
		String myClusterName = "cluster-e42e";
		String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);
		String mainClass = "invertedIndices";
		String projectId = "cs1660-293317";

		try {
			JobControllerSettings jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
			JobControllerClient jobControllerClient = JobControllerClient.create(jobControllerSettings);
			
			JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(myClusterName).build();
			HadoopJob myJob = HadoopJob.newBuilder().setMainClass(mainClass)
					.setMainClass("invertedIndices")
					.addJarFileUris("gs:dataproc-staging-us-east1-469710197436-tqybsejiR/Project/invertedIndices.jar")
					.addArgs("gs://dataproc-staging-us-east1-469710197436-tqybseji/Project/input")
					.addArgs("gs://dataproc-staging-us-east1-469710197436-tqybseji/Project/output")
					.build();
			Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(myJob).build();
			Job request = jobControllerClient.submitJob(projectId, region, job);

			String jobId = request.getReference().getJobId();
            System.out.println(String.format("Submitted job " + jobId));
        } catch(Exception e) {}
			
    }
    public static void main(String args[]){
        searchEngine searchEngine = new searchEngine();
     }

     
}
