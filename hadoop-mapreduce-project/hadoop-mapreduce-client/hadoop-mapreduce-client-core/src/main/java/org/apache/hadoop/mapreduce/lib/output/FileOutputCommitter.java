/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.output;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartResult;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.osslib.OSSClientAgent;
import org.apache.hadoop.mapreduce.lib.output.osslib.Result;
import org.apache.hadoop.mapreduce.lib.output.osslib.TaskEngine;
import org.apache.hadoop.mapreduce.lib.output.osslib.task.OSSCommitTask;
import org.apache.hadoop.mapreduce.lib.output.osslib.task.Task;

/** An {@link OutputCommitter} that commits files specified 
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends OutputCommitter {
  private static final Log LOG = LogFactory.getLog(FileOutputCommitter.class);

  /** 
   * Name of directory where pending data is placed.  Data that has not been
   * committed yet.
   */
  public static final String PENDING_DIR_NAME = "_temporary";
  /**
   * Temporary directory name 
   *
   * The static variable to be compatible with M/R 1.x
   */
  @Deprecated
  protected static final String TEMP_DIR_NAME = PENDING_DIR_NAME;
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
      "mapreduce.fileoutputcommitter.marksuccessfuljobs";
  public static final String FILEOUTPUTCOMMITTER_ALGORITHM_VERSION =
      "mapreduce.fileoutputcommitter.algorithm.version";
  public static final int FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 1;
  protected Path outputPath = null;
  protected Path workPath = null;
  private final int algorithmVersion;
  private Configuration conf = null;
  private OSSClientAgent ossClientAgent = null;
  private List<Path> uploadIdFiles = new ArrayList<Path>();
  private int numCommitThreads = 1;

  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  public FileOutputCommitter(Path outputPath, 
                             TaskAttemptContext context) throws IOException {
    this(outputPath, (JobContext)context);
    if (outputPath != null) {
      workPath = getTaskAttemptPath(context, outputPath);
    }
  }
  
  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  @Private
  public FileOutputCommitter(Path outputPath, 
                             JobContext context) throws IOException {
    this.conf = context.getConfiguration();
    this.numCommitThreads = conf.getInt("fs.oss.uploadPartCommit.thread.number", 10);
    algorithmVersion =
        conf.getInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
                    FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT);
    LOG.info("File Output Committer Algorithm version is " + algorithmVersion);
    if (algorithmVersion != 1 && algorithmVersion != 2) {
      throw new IOException("Only 1 or 2 algorithm version is supported");
    }
    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      this.outputPath = fs.makeQualified(outputPath);
    }
  }
  
  /**
   * @return the path where final output of the job should be placed.  This
   * could also be considered the committed application attempt path.
   */
  private Path getOutputPath() {
    return this.outputPath;
  }
  
  /**
   * @return true if we have an output path set, else false.
   */
  protected boolean hasOutputPath() {
    return this.outputPath != null;
  }
  
  /**
   * @return the path where the output of pending job attempts are
   * stored.
   */
  private Path getPendingJobAttemptsPath() {
    return getPendingJobAttemptsPath(getOutputPath());
  }
  
  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  private static Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, PENDING_DIR_NAME);
  }
  
  /**
   * Get the Application Attempt Id for this job
   * @param context the context to look in
   * @return the Application Attempt Id for a given job.
   */
  private static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(context, getOutputPath());
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @param out the output path to place these in.
   * @return the path to store job attempt data.
   */
  public static Path getJobAttemptPath(JobContext context, Path out) {
    return getJobAttemptPath(getAppAttemptId(context), out);
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getJobAttemptPath(appAttemptId, getOutputPath());
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  private static Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out), String.valueOf(appAttemptId));
  }
  
  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks. 
   * @return the path where the output of pending task attempts are stored.
   */
  private Path getPendingTaskAttemptsPath(JobContext context) {
    return getPendingTaskAttemptsPath(context, getOutputPath());
  }
  
  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks. 
   * @return the path where the output of pending task attempts are stored.
   */
  private static Path getPendingTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getJobAttemptPath(context, out), PENDING_DIR_NAME);
  }
  
  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * 
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return new Path(getPendingTaskAttemptsPath(context), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * 
   * @param context the context of the task attempt.
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   */
  public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * Compute the path where the output of a committed task is stored until
   * the entire job is committed.
   * @param context the context of the task attempt
   * @return the path where the output of a committed task is stored until
   * the entire job is committed.
   */
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return getCommittedTaskPath(getAppAttemptId(context), context);
  }
  
  public static Path getCommittedTaskPath(TaskAttemptContext context, Path out) {
    return getCommittedTaskPath(getAppAttemptId(context), context, out);
  }
  
  /**
   * Compute the path where the output of a committed task is stored until the
   * entire job is committed for a specific application attempt.
   * @param appAttemptId the id of the application attempt to use
   * @param context the context of any task.
   * @return the path where the output of a committed task is stored.
   */
  protected Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }
  
  private static Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context, Path out) {
    return new Path(getJobAttemptPath(appAttemptId, out),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  private static class CommittedTaskFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return !PENDING_DIR_NAME.equals(path.getName());
    }
  }

  /**
   * Get a list of all paths where output from committed tasks are stored.
   * @param context the context of the current job
   * @return the list of these Paths/FileStatuses. 
   * @throws IOException
   */
  protected FileStatus[] getAllCommittedTaskPaths(JobContext context)
    throws IOException {
    Path jobAttemptPath = getJobAttemptPath(context);
    FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
    return fs.listStatus(jobAttemptPath, new CommittedTaskFilter());
  }

  /**
   * Get the directory that the task should write results into.
   * @return the work directory
   * @throws IOException
   */
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  /**
   * Create the temporary directory that is the root of all of the task 
   * work directories.
   * @param context the job's context
   */
  public void setupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path jobAttemptPath = getJobAttemptPath(context);
      FileSystem fs = jobAttemptPath.getFileSystem(
          context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        LOG.error("Mkdirs failed to create " + jobAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in setupJob()");
    }
  }

  /**
   * The job has completed so move all committed tasks to the final output dir.
   * Delete the temporary directory, including all of the work directories.
   * Create a _SUCCESS file to make it as successful.
   * @param context the job's context
   */
  public void commitJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path finalOutput = getOutputPath();
      Configuration conf = context.getConfiguration();
      // set 'fs.oss.reader.concurrent.number' = 2 for fear of two many threads when do oss commit.
      conf.setInt("mapreduce.ossreader.algorithm.version", 2);
      FileSystem fs = finalOutput.getFileSystem(conf);

      uploadIdFiles.clear();
      if (algorithmVersion == 1) {
        for (FileStatus stat: getAllCommittedTaskPaths(context)) {
          mergePaths(fs, stat, finalOutput);
        }
      }
      multipartCommit(fs);
      // delete the _temporary folder and create a _done file in the o/p folder
      cleanupJob(context);
      // True if the job requires output.dir marked on successful job.
      // Note that by default it is set to true.
      if (context.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
        Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
        fs.create(markerPath).close();
      }
    } else {
      LOG.warn("Output Path is null in commitJob()");
    }
  }

  private void multipartCommit(FileSystem fs) throws IOException {
      if (!uploadIdFiles.isEmpty()) {
        if (ossClientAgent == null) {
          try {
            ossClientAgent = getOSSClientAgent();
          } catch (Exception e) {
            throw new IOException("Failed to initialize an OSS client", e);
          }
        }

        if (algorithmVersion == 1) {
          // use multi-thread to do multipart commit.
          List<Task> tasks = new ArrayList<Task>();
          List<List<Path>> subLists = bisect(uploadIdFiles, numCommitThreads);
          for (int i = 0; i < numCommitThreads; i++) {
            Task ossCommitTask = new OSSCommitTask(fs, ossClientAgent, subLists.get(i));
            ossCommitTask.setUuid(i + "");
            tasks.add(ossCommitTask);
          }
          TaskEngine taskEngine = new TaskEngine(tasks, numCommitThreads, numCommitThreads);
          try {
            taskEngine.executeTask();
            Map<String, Object> responseMap = taskEngine.getResultMap();
            for (int i = 0; i < tasks.size(); i++) {
              Result result = (Result) responseMap.get(i + "");
              if (!result.isSuccess()) {
                throw new IOException("Failed to complete MultipartUpload");
              }
            }
          } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
          } finally {
            taskEngine.shutdown();
          }
        } else {
          InputStream in;
          ObjectInputStream ois = null;
          String bucket;
          String finalDstKey = "";
          String uploadId = "";
          try {
            for (Path path : uploadIdFiles) {
              in = fs.open(path);
              ois = new ObjectInputStream(in);

              bucket = (String) ois.readObject();
              finalDstKey = (String) ois.readObject();
              uploadId = (String) ois.readObject();
              List<SerializableETag> serializableETags = (List<SerializableETag>) ois.readObject();
              List<PartETag> partETags = new ArrayList<PartETag>();
              for (SerializableETag serializableETag : serializableETags) {
                partETags.add(serializableETag.toPartETag());
              }
              CompleteMultipartUploadResult completeMultipartUploadResult =
                      ossClientAgent.completeMultipartUpload(bucket, finalDstKey, uploadId, partETags);
              LOG.info("complete multi-part upload " + uploadId + ": [" + completeMultipartUploadResult.getETag() + "]");
              fs.delete(path, true);
              ois.close();
            }
          } catch (Exception e) {
            LOG.error("Failed to complete multi-part " + uploadId + ", key: " + finalDstKey, e);
            throw new IOException("Failed to complete MultipartUpload");
          } finally {
            if (ois != null) {
              try {
                ois.close();
              } catch (IOException e) {
                e.printStackTrace();
                LOG.error("Failed to close oss input stream");
              }
            }
          }
        }
        uploadIdFiles.clear();
    }
  }

  private List<List<Path>> bisect(List<Path> paths, int slice) {
    List<List<Path>> ret = new ArrayList<List<Path>>();
      if (paths != null && paths.size() > 0) {
        for (int i=0; i<slice; i++) {
          ret.add(new ArrayList<Path>());
        }
        int n = 0;
        for(Path path: paths) {
          int idx = n % slice;
          ret.get(idx).add(path);
          n++;
        }
      }

    return ret;
  }

  /**
   * Merge two paths together.  Anything in from will be moved into to, if there
   * are any name conflicts while merging the files or directories in from win.
   * @param fs the File System to use
   * @param from the path data is coming from.
   * @param to the path data is going to.
   * @throws IOException on any error
   */
  protected void mergePaths(FileSystem fs, final FileStatus from,
      final Path to) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Merging data from " + from + " to " + to);
    }
    FileStatus toStat;
    try {
      toStat = fs.getFileStatus(to);
    } catch (FileNotFoundException fnfe) {
      toStat = null;
    }

    if (from.isFile() && from.getPath().getName().endsWith(".upload")) {
      this.uploadIdFiles.add(from.getPath());
    } else if (from.isFile()) {
      if (toStat != null) {
        if (!fs.delete(to, true)) {
          throw new IOException("Failed to delete " + to);
        }
      }

      if (!fs.rename(from.getPath(), to)) {
        throw new IOException("Failed to rename " + from + " to " + to);
      }
    } else if (from.isDirectory()) {
      if (toStat != null) {
        if (!toStat.isDirectory()) {
          if (!fs.delete(to, true)) {
            throw new IOException("Failed to delete " + to);
          }
          renameOrMerge(fs, from, to);
        } else {
          //It is a directory so merge everything in the directories
          for (FileStatus subFrom : fs.listStatus(from.getPath())) {
            Path subTo = new Path(to, subFrom.getPath().getName());
            mergePaths(fs, subFrom, subTo);
          }
        }
      } else {
        renameOrMerge(fs, from, to);
      }
    }
  }

  private void renameOrMerge(FileSystem fs, FileStatus from, Path to)
      throws IOException {
    if (algorithmVersion == 1) {
      if (!fs.rename(from.getPath(), to)) {
        throw new IOException("Failed to rename " + from + " to " + to);
      }
    } else {
      fs.mkdirs(to);
      for (FileStatus subFrom : fs.listStatus(from.getPath())) {
        Path subTo = new Path(to, subFrom.getPath().getName());
        mergePaths(fs, subFrom, subTo);
      }
    }
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path pendingJobAttemptsPath = getPendingJobAttemptsPath();
      FileSystem fs = pendingJobAttemptsPath
          .getFileSystem(context.getConfiguration());
      cleanOSSUploadResidue(pendingJobAttemptsPath, fs);
      fs.delete(pendingJobAttemptsPath, true);
    } else {
      LOG.warn("Output Path is null in cleanupJob()");
    }
  }

  /**
   * Delete the temporary directory, including all of the work directories.
   * @param context the job's context
   */
  @Override
  public void abortJob(JobContext context, JobStatus.State state) 
  throws IOException {
    // delete the _temporary folder
    cleanupJob(context);
  }
  
  /**
   * No task setup required.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the 
    // task is writing.
  }

  /**
   * Move the files from the work directory to the job output directory
   * @param context the task context
   */
  @Override
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    commitTask(context, null);
  }

  @Private
  public void commitTask(TaskAttemptContext context, Path taskAttemptPath) 
      throws IOException {

    TaskAttemptID attemptId = context.getTaskAttemptID();
    if (hasOutputPath()) {
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      Configuration conf = context.getConfiguration();
      // set 'fs.oss.reader.concurrent.number' = 2 for fear of two many threads when do oss commit.
      conf.setInt("mapreduce.ossreader.algorithm.version", 2);
      FileSystem fs = taskAttemptPath.getFileSystem(conf);
      FileStatus taskAttemptDirStatus;
      try {
        taskAttemptDirStatus = fs.getFileStatus(taskAttemptPath);
      } catch (FileNotFoundException e) {
        taskAttemptDirStatus = null;
      }

      if (taskAttemptDirStatus != null) {
        if (algorithmVersion == 1) {
          Path committedTaskPath = getCommittedTaskPath(context);
          if (fs.exists(committedTaskPath)) {
             if (!fs.delete(committedTaskPath, true)) {
               throw new IOException("Could not delete " + committedTaskPath);
             }
          }
          if (!fs.rename(taskAttemptPath, committedTaskPath)) {
            throw new IOException("Could not rename " + taskAttemptPath + " to "
                + committedTaskPath);
          }
          LOG.info("Saved output of task '" + attemptId + "' to " +
              committedTaskPath);
        } else {
          // directly merge everything from taskAttemptPath to output directory
          uploadIdFiles.clear();
          mergePaths(fs, taskAttemptDirStatus, outputPath);
          LOG.info("Saved output of task '" + attemptId + "' to " +
              outputPath);
          multipartCommit(fs);
        }
      } else {
        LOG.warn("No Output found for " + attemptId);
      }
    } else {
      LOG.warn("Output Path is null in commitTask()");
    }
  }

  /**
   * Delete the work directory
   * @throws IOException 
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    abortTask(context, null);
  }

  @Private
  public void abortTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    if (hasOutputPath()) { 
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      cleanOSSUploadResidue(taskAttemptPath, fs);
      if(!fs.delete(taskAttemptPath, true)) {
        LOG.warn("Could not delete "+taskAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in abortTask()");
    }
  }

  /**
   * Did this task write any files in the work directory?
   * @param context the task's context
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context
                                 ) throws IOException {
    return needsTaskCommit(context, null);
  }

  @Private
  public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath
    ) throws IOException {
    if(hasOutputPath()) {
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      return fs.exists(taskAttemptPath);
    }
    return false;
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return true;
  }
  
  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
    if(hasOutputPath()) {
      context.progress();
      TaskAttemptID attemptId = context.getTaskAttemptID();
      int previousAttempt = getAppAttemptId(context) - 1;
      if (previousAttempt < 0) {
        throw new IOException ("Cannot recover task output for first attempt...");
      }

      Path previousCommittedTaskPath = getCommittedTaskPath(
          previousAttempt, context);
      Configuration conf = context.getConfiguration();
      // set 'fs.oss.reader.concurrent.number' = 2 for fear of two many threads when do oss commit.
      conf.setInt("mapreduce.ossreader.algorithm.version", 2);
      FileSystem fs = previousCommittedTaskPath.getFileSystem(conf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to recover task from " + previousCommittedTaskPath);
      }
      if (algorithmVersion == 1) {
        if (fs.exists(previousCommittedTaskPath)) {
          Path committedTaskPath = getCommittedTaskPath(context);
          if (fs.exists(committedTaskPath)) {
            if (!fs.delete(committedTaskPath, true)) {
              throw new IOException("Could not delete "+committedTaskPath);
            }
          }
          //Rename can fail if the parent directory does not yet exist.
          Path committedParent = committedTaskPath.getParent();
          fs.mkdirs(committedParent);
          if (!fs.rename(previousCommittedTaskPath, committedTaskPath)) {
            throw new IOException("Could not rename " + previousCommittedTaskPath +
                " to " + committedTaskPath);
          }
        } else {
            LOG.warn(attemptId+" had no output to recover.");
        }
      } else {
        // essentially a no-op, but for backwards compatibility
        // after upgrade to the new fileOutputCommitter,
        // check if there are any output left in committedTaskPath
        if (fs.exists(previousCommittedTaskPath)) {
          LOG.info("Recovering task for upgrading scenario, moving files from "
              + previousCommittedTaskPath + " to " + outputPath);
          FileStatus from = fs.getFileStatus(previousCommittedTaskPath);
          uploadIdFiles.clear();
          mergePaths(fs, from, outputPath);
          multipartCommit(fs);
        }
        LOG.info("Done recovering task " + attemptId);
      }
    } else {
      LOG.warn("Output Path is null in recoverTask()");
    }
  }

  public OSSClientAgent getOSSClientAgent() throws Exception {
    OSSClientAgent ossClientAgent;
    String accessKeyId = null;
    String accessKeySecret = null;
    String securityToken = null;
    String endpoint = null;

    URI uri = outputPath.toUri();
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("Invalid hostname in URI " + uri);
    }
    String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      String[] ossCredentials  = userInfo.split(":");
      if (ossCredentials.length >= 2) {
        accessKeyId = ossCredentials[0];
        accessKeySecret = ossCredentials[1];
      }
      if (ossCredentials.length == 3) {
        securityToken = ossCredentials[2];
      }
    }

    String bucket;
    String host = uri.getHost();
    if (!StringUtils.isEmpty(host) && !host.contains(".")) {
      bucket = host;
    } else if (!StringUtils.isEmpty(host)){
      bucket = host.substring(0, host.indexOf("."));
      endpoint = host.substring(host.indexOf(".")+1);
    }

    if (accessKeyId == null) {
      accessKeyId = conf.getTrimmed("fs.oss.accessKeyId");
    }
    if (accessKeySecret == null) {
      accessKeySecret = conf.getTrimmed("fs.oss.accessKeySecret");
    }
    if (securityToken == null) {
      securityToken = conf.getTrimmed("fs.oss.securityToken");
    }
    if (endpoint == null) {
      endpoint = conf.getTrimmed("fs.oss.endpoint");
    }

    if (securityToken == null) {
      ossClientAgent = new OSSClientAgent(endpoint, accessKeyId, accessKeySecret, conf);
    } else {
      ossClientAgent = new OSSClientAgent(endpoint, accessKeyId, accessKeySecret, securityToken, conf);
    }

    return ossClientAgent;
  }

  private void cleanOSSUploadResidue(Path jobPath, FileSystem fs) throws IOException {
    for(FileStatus fileStatus: fs.listStatus(jobPath)) {
      LOG.info("cleaning "+fileStatus.getPath().getName());
      if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".upload")) {
        if(ossClientAgent == null) {
          try {
            ossClientAgent = getOSSClientAgent();
          } catch (Exception e) {
            LOG.error("can not initialize OSSClientAgent, "+e.getMessage());
            throw new IOException(e);
          }
        }
        InputStream in = fs.open(fileStatus.getPath());
        ObjectInputStream ois = new ObjectInputStream(in);
        try {
          String bucket = (String) ois.readObject();
          String finaleDstKey = (String) ois.readObject();
          String uploadId = (String) ois.readObject();
          ossClientAgent.abortMultipartUpload(bucket, finaleDstKey, uploadId);
        } catch (ClassNotFoundException e) {
          LOG.error(e.getMessage());
          throw new IOException("Can not read object from inputStream", e);
        } finally {
          ois.close();
        }
      } else if (fileStatus.isDirectory()) {
        cleanOSSUploadResidue(fileStatus.getPath(), fs);
      }
    }
  }
}