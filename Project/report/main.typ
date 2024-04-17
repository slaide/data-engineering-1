#set heading(numbering: "1.1")

#let comment(t,display:false)={
  if display {t}
}

#comment(display:false)[
    == problem
    
    we have a LOT of images. we have terabytes of images. the computational pipeline to retrieve the morphology data from these images is resource intensive. the software we use there is called cellprofiler, and it takes a lot of time to run, and crashes regularly.
    
    == solution
    
    use #link("https://docs.docker.com/engine/swarm/")[docker swarm] to orchestrate and scale the following architecture:
    
    + store images in dedicated storage device (e.g. object storage, or some other central storage server). #link("https://docs.localstack.cloud/user-guide/aws/s3/")[localstack] seems like a good solution.
    + images are recorded in a database (some sql database, e.g. #link("https://mariadb.org/")[mariadb]), with submission data and other metadata (e.g. to be able to tell which images belong together, like same site in same well on same plate)
    + a worker queries the database regularly, checks for new (complete!) submission of images, and creates a new workload to be processed by a cellprofiler pipeline for each set of images. this can be just a python script.
    + a message queue (like #link("https://rabbitmq-website.pages.dev/")[RabbitMQ]) accepts workload messages and distributes them to workers
    + any number of worker nodes (docker containers) accept messages and run #link("https://cellprofiler.org/")[cellprofiler] in the workload. they fetch images from the database, calculate the data, write the result back to the storage server and submit an entry to the database.
    
    salman mentioned #link("https://docs.celeryq.dev/en/stable/")[celery] to scale the worker nodes. looks like a rabbitmq python wrapper (to make the existing rabbitmq python api easier to use).
    
    == mini presentation
    
    attended the session 14.15-15.00
]

#align(center, text(17pt)[
  *Distributed Cellprofiler*
])
#align(center,[Patrick Hennig, 2024-04-17])
#align(center,[Project report for Data Science I])

= Background
#comment[Description of the scientific area/problem setting that gave rise to the dataset. Place the dataset in context, providing sufficient references for the reader to understand the importance and significance of the data. What kind of analyses have been conducted in literature?]

Investigating the precise mechanism with which a drug impacts a cell is complicated but necessary to find new drug candidates. There are other ways to identify a compound that may be used to treat a disease, like live-dead essays to determine cell toxicity, but this may not be enough to find _good_ drug candidates. A better approach describes the molecular interactions with a cell. This is usually a very time and work intensive approach. One method that balances the need for fast evaluation with deep insight into the mechanism is morphological cell profiling, facilitated by cell painting.

Morphological profiling is a method that can assign each cell in a sample a value in high dimensional numerical space. This information can be gathered through cell painting, where different cell compartments are stained with fluorescent dyes to make them exclusively visible with fluorescence microscopy. The individual signals from the cell compartments in individual images are then used to calculate this numerical representation, using a software like cellprofiler #cite(<cellprofiler>).

To then find a good drug candidate, you start by measuring the cell morphology for known compounds that have the effect on a cell you want. Based on other factors, like similar molecular structure, you identify a large library of new compounds that might exhibit this same effect, and measure their influence on the cell morphology. To identify the likely molecular mechanism of action for these new compounds in practice, you identify the _known_ compounds with morphology information closest to these new data points. This method scales well to large numbers of compounds, but it is resource intensive. The software algorithm to calculate the features from image data scales well horizontally, because the representations of cells are not correlated within the images, so all images of all cells treated by any number of drugs can be processed asynchronously.

A related challenge for this method is that it usually requires large amounts of storage for the large amount of image data. Even with data compression techniques, data is in the order of TB per experiment. This data is then distributed to workers that calculate the cell features from the image data. To retrieve meaningful results from the cell representations, there is also metadata for each cell that needs to be stored with the numerical representation and the images, and other processing steps to reduce the large amount of data to something a human can infer information from.

= Data Format
#comment[Describe the data format(s) used in the dataset. Put them in context: why were the specific formats chosen, and would there be alternatives? What are the pros and cons of the formats used?]

The main data format in this project is TIFF #footnote[USA Library of Congress TIFF format overview https://www.loc.gov/preservation/digital/formats/fdd/fdd000022.shtml], used by the image files. This is a common image file format for scientific applications, which supports different kinds of compression. For this experiment, the data uses the lossless compression method LZW, reducing the raw data size by about 50%. Each image is 2500px by 2500px in size, with 16bit monochrome pixel depth. The metadata is part csv files, part json. All these human readable files are in total about the same size as a single image, so compression has no real influence on the size of the total dataset.

An alternative for the LWZ TIFF image files could be lossy compression in any file format (e.g. TIFF, or JPEG #footnote[USA Library of Congress JPEG format overview https://www.loc.gov/preservation/digital/formats/fdd/fdd000017.shtml]). Lossy compression means information is actually lost though, so this is not a real option for detailed image data, even though the compression ratio may be an order of magnitude. A more capable alternative is other image formats with lossless compression, like PNG #footnote[USA Library of Congress PNG format overview https://www.loc.gov/preservation/digital/formats/fdd/fdd000153.shtml], but these are not as common in scientific software support.

The numerical representation of the cells is stored in parquet #footnote[Apache Parquet format specification https://github.com/apache/parquet-format] files. The native output format of the cellprofiler software is csv files, but with numbers of cells in the order of millions for regular experiments, this is a huge waste of memory. the parquet file format compresses this data from text representation of numbers to binary representations that save enormous amounts of storage space, and also structure the data better for much improved load/store performance.

= Computational Experiments
#comment[This is the main section of the report. Describe and motivate the choice of tools and the distributed system that you designed. Describe how you have designed your scalability experiments, and present the results. Think carefully about suitable ways to illustrate the scalability of your solution.]

I will perform a series of computation experiments in this project. These experiment will be quite similar, performing identical computations on the same dataset, but with a variable number of worker nodes operating on the data in parallel. To facilitate this distribution of the workload across any number of worker nodes, I have set up a selection of multiple services for different compute, storage, and user interface purposes.

== Software Choices

The system I have developed has six main components. Each component is implemented as a separate docker #cite(<docker>) service in a docker service stack, which in turn is orchestrated through docker swarm #footnote[Docker Swarm documentation https://docs.docker.com/engine/swarm/]. Some of the services are binary applications, others implement functionality through python #cite(<python>) code. The services communicate with each other over network ports, using a variety of protocols.

=== Object Storage
To store the large amount of data, i.e. image and parquet files, I have chosen an object storage solution. For local testing, the localstack #cite(<localstack>) project offers an AWS S3 compatible component with a pre-made docker definition for easy deployment and configuration. This service requires very little configuration, all of which is implemented inside the docker service definition.

=== Structured Metadata Storage
To store all required metadata regarding cell morphology projects, experiments, files etc. in the object storage, I have chosen MariaDB #cite(<mariadb>), an SQL database. This software also has a pre-made dockerfile available for fast deployment and easy configuration. It does not require much direct configuration, but the specification of the internal data structure at runtime is quite complex. This runtime database structure is defined in python client code.

=== Task Queue
The execution of compute intensive tasks is queried via a central task queue, offered by RabbitMQ #cite(<rabbitmq>). The latency of this workload is too high for remote procedure calls (RPC) awaiting direct call results, so computation is initiated via a message, and results are stored in the database, where they can be retrieved later. The current state of computations is tracked in the database, allowing any service to query the status of ongoing calculations without interacting with a worker directly. RabbitMQ offers a pre-made docker container, which requires very little configuration. To simplify message handling in the services interacting with this queue, celery #cite(<celery>) is used. Note that celery operates purely on top of RabbitMQ, in python code. Celery does not offer functionality that is fundamentally not already available in RabbitMQ.

=== Cellprofiler Worker
Cellprofiler #cite(<cellprofiler>) is used here to calculate the numerical representation of cell morphology from the image data. This software is technically more capable than the single-threaded operation on a single image set implemented here, but it is somewhat unstable and does not scale across multiple nodes, so it is used here as a small service. Operating on a single image set at any time allows fast recovery from failure. Input and output for this software is done via the filesystem, so the python wrapper code in the service handles transmission of files between the local filesystem and the object storage.

To briefly summarize the workflow by these worker services:
+ the worker awaits a message from the task queue, containing
    - some image file related metadata
    - the location of several image files, to run cellprofiler on
    - metadata identifying this cellprofiler invocation, used to identify the eventual result file location
+ fetch the image files from the object storage
+ invoke cellprofiler through the command line interface
+ convert the output files from csv to parqet
+ write the result files back to object storage, with result file metadata written to the database

=== Cell Representation Display
At any point during an experiment, regardless of the fraction of the total dataset that has completed processing, a user may request a visualization of the current results. The specific visualization is an ongoing research question, but briefly, the cell morphology data is processed by a dimensionality reduction method to gather insights into the effects of the drugs used to treat the cells.

This insight may also be used to debug the cell treatment process in the lab. If certain expectations regarding the data are not met, there may have been issues in the sample preparation. Visualizing the data not only after processing the whole dataset is therefore quite advantageous, because some obvious issues may be caught early, not only when processing is done after a potentially very long time.

The specific mechanism of this visulisation is as follows:
+ a message arrives, identifying:
    - an experiment for which to visualise the results
    - expected location for final plot
+ fetch information about all result files for this experiment from the database
+ download the files from the object storage into local storage
+ concatenate all local result files and process into a low dimensional representation
+ generate an interactive plot, store it in object storage at target location

The cell morphology data is processed via dataframes, used by polars #cite(<polars>) and pandas #cite(<pandas>). The plots are generated with plotly #cite(<plotly>), which allows saving them as HTML files for interactive display in the browser.

=== System User Interface
To facilitate user interaction with this system, I have written a web interface. The interface enables basic data ingest and result retrieval through a form. Processing of the image data is automatically initiated upon upload. The frontend also gives information about the current processing status of an experiment. The final result plot may be downloaded as a file or interacted with, directly in the interface. The server for this web interface is written in python, using the flask #cite(<flask>) framework.

=== Central Code Components
Many system components, namely the cellprofiler service, result visualisation service and web interface, all interact with the object storage and the database. This also implies handling metadata for all data in the system. To reduce the friction of interaction across process boundaries I have written a python package that is shared by all these services, combined with service specific code. This code interacts with the object storage via #cite(<boto3>).

== Scaling Experiment

The essential performance evaluation of this system focuses on horizontal scalin. I have a dataset of fixed size, and exactly one service in the system that takes up (nearly) all the compute time. I will measure the wall time required to complete the calculations for this dataset, and see how this time changes with the number of parallel workers, i.e. worker service replicates. Ideally, the wall time would be inversely proportional to the number of workers.

More specifically, I am using a dataset with 228 atomic units of work, which are roughly equal in workload, distributed via a central task queue (RabbitMQ). The tasks from the task queue are fetched by the workers directly. I will process the dataset with 1 worker, then 2, 4, 6 and 8.

The results are seen in the table and graph below, where we can see a linear correlation between the number of workers and wall time used. The throughput rate per worker increases slightly with the number of workers, indicating a system overhead that is shared by the whole system.

#comment(display:false)[
  1 worker: 8:07-10:11=124 minutes
  2 workers: 16.30-17.35=65 minutes
  4 workers: 13.40-14.14=34 minutes
  6 workers: 14.45-15.10=25 minutes
  8 workers: 15.08-15.26=18 minutes
]

// hours to minutes factor (commonly known number, but good to document which units are being converted)
#let h2m=60

#let ts(f)={
  // convert a time-like float to time float (e.g. 8.15 as 8:15 -> 8.25)
  // (ts for timestamp)
  let hour=calc.floor(f)
  let minutes=f - hour
  return hour+minutes/0.6
}

#let num_image_sets=228
#let measurements=(
  (1,  8.07, 10.11),
  (2, 16.30, 17.35),
  (4, 13.40, 14.14),
  (6, 14.45, 15.10),
  (8, 15.08, 15.26),
)

#let truncate_number(n,num_digits:2,round:true,separator:".")={
  // returns a text representation of the number, with the specified decimal digits
  
  // if num_digits:0, there is no trailing decimal separator
  
  //truncates if round:false
  //separator can be a string of any length
  //num_digits must be positive (zero is allowed)
  
  if round{
    // round to target number of digits
    n=calc.round(n,digits:num_digits)
  }
  
  let s=str(n)
  let parts=s.split(".")

  if num_digits==0{
    return parts.at(0)
  }
  
  if parts.len() == 1{
    parts.push("0")
  }
  
  let current_num_digits=parts.at(1).len()
  if current_num_digits < num_digits{
    parts.at(1)=parts.at(1)+"0"*(num_digits - current_num_digits)
  }
  
  return parts.at(0)+separator+parts.at(1)
}

#let linear_regression(points)={
  let n = points.len()
  let sum_x=0
  let sum_y=0
  let sum_x2=0
  let sum_xy=0
  for point in points{
    sum_x+=point.at(0)
    sum_y+=point.at(1)
    sum_x2+=calc.pow(point.at(0),2)
    sum_xy+=point.at(0)*point.at(1)
  }
  
  let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - calc.pow(sum_x,2))
  let intercept = (sum_y - slope * sum_x) / n
  
  return (slope, intercept)
}

#{
  let table_arg_from_measurements(m)={
    let (num_workers,start_time,end_time)=m

    let duration_minutes=(ts(end_time) - ts(start_time))*h2m
    let sets_per_minute=num_image_sets/duration_minutes
    return (
      truncate_number(num_workers,num_digits:0),
      truncate_number(start_time,separator:":"),
      truncate_number(end_time,separator:":"),
      truncate_number(duration_minutes),
      truncate_number(sets_per_minute/num_workers)
    )
  }
  let table_args=measurements.map(table_arg_from_measurements).flatten()

  figure(
    table(
      columns: 5,
      
      inset: 10pt,
      align: horizon,
      table.header(
        [*Num Workers*], [*Start time*], [*End time*], [*Duration [minutes]*], [*\# Image sets per minute per worker*]
      ),
      
      ..table_args
    ),
    caption: [Raw experiment results, plus some simple derived values. The number of workers indicates the number of worker services (containers) used in parallel. The start and end times are timestamps. The duration and number of image sets per minute per work are derived from the three left-most columns.]
  )
}

#import "@preview/cetz:0.2.2"

#figure(
cetz.canvas({
  let plot_width=400pt
  let plot_height=140pt
  
  let bb_min=(0,0)
  let bb_max=(8,15)

  let plot_axis_scale_line_length=3pt
  
  let x_min=bb_min.at(0)
  let x_max=bb_max.at(0)
  // x axis marker information
  let x_num_steps=5
  let x_axis_is_float=false
  let x_delta=(x_max - x_min)/(x_num_steps - 1)
  
  let y_min=bb_min.at(1)
  let y_max=bb_max.at(1)
  // y axis marker information
  let y_num_steps=4
  let y_axis_is_float=true
  let y_delta=(y_max - y_min)/(y_num_steps - 1)

  // decide if axis labels should have a decimal point or not
  let x_num_digits=1
  if not x_axis_is_float{
    x_num_digits=0
  }
  let y_num_digits=1
  if not y_axis_is_float{
    y_num_digits=0
  }

  let plot_x_extent=x_max - x_min
  let plot_y_extent=y_max - y_min
  
  let plot_x_offset=10pt
  let plot_y_offset=-10pt

  let get_x(x,ignore_offset:false)={
    // with imagined x value, get x display coordinate
    let ret=plot_width*x/plot_x_extent
    if ignore_offset{
      return ret
    }
    return ret + plot_x_offset
  }
  let get_y(y,ignore_offset:false)={
    // with imagined y value, get y display coordinate
    let ret=plot_height*y/plot_y_extent
    if ignore_offset{
      return ret
    }
    return ret + plot_y_offset
  }
  let get_point(point,ignore_offset:false)={
    return (get_x(point.at(0), ignore_offset:ignore_offset), get_y(point.at(1), ignore_offset:ignore_offset))
  }

  // make background grid
  cetz.draw.grid(
    get_point(bb_min),
    get_point(bb_max),
    step: get_point((x_delta,y_delta),ignore_offset:true),
    stroke: gray + 0.2pt
  )

  // x axis arrow
  cetz.draw.line((get_x(x_min), get_y(0)), (get_x(x_max), get_y(0)))
  //cetz.draw.content((), $ x $, anchor: "west")
  // y axis arrow
  cetz.draw.line((get_x(0), get_y(y_min)), (get_x(0), get_y(y_max)))
  //cetz.draw.content((), $ y $, anchor: "south")
  
  // x axis markers
  let x=x_min
  while x <= x_max {
    let draw_x=get_x(x)

    cetz.draw.line((draw_x, get_y(0)+plot_axis_scale_line_length), (draw_x, get_y(0)-plot_axis_scale_line_length))

    cetz.draw.content((), anchor: "north", truncate_number(x,num_digits:x_num_digits))
    x += x_delta
  }

  // y axis markers
  let y=y_min
  while y <= y_max {
    let draw_y=get_y(y)
    cetz.draw.line((plot_axis_scale_line_length+get_x(0), draw_y), (-plot_axis_scale_line_length+get_x(0), draw_y))

    cetz.draw.content((), anchor: "east", truncate_number(y,num_digits:y_num_digits))
    y += y_delta
  }

  
  let point(x,y,radius:1,..args)={
    let radius_scale=3pt // independent of graph size!
    cetz.draw.circle(get_point((x,y)),radius:radius*radius_scale,..args)
  }

  let line(from,to,..args)={
    cetz.draw.line(
      get_point(from),
      get_point(to),
      ..args
    )
  }
  
  // x axis label
  cetz.draw.content((get_x(x_min+plot_x_extent/2),get_y(0)-20pt),text[\# Worker Nodes],anchor:"north")
  // y axis label
  cetz.draw.content((get_x(0)-35pt,get_y(y_min+plot_y_extent/2)),text[\# Image Sets per minute],anchor:"north",angle:90deg)
  // set title
  cetz.draw.content((get_x(x_min+plot_x_extent/2),get_y(y_max)+8pt),text(size:17pt)[Horizontal scaling results],anchor:"south")
  
  // actually draw stuff

  let points=()
  for (num_workers,start_time,end_time) in measurements{
    let duration_m=(ts(end_time) - ts(start_time))*h2m
    point(num_workers,num_image_sets/duration_m)
    points.push((num_workers,num_image_sets/duration_m))
  }

  let get_linear(intercept,slope,x)={
    let y=intercept+slope*x
    return (x,y)
  }

  let (slope,intercept)=linear_regression(points)
  let lower=get_linear(intercept,slope,0)

  let max_num_workers=measurements.at(-1).at(0)
  let upper=get_linear(intercept,slope,max_num_workers)
  line(lower,upper,stroke:(dash:"dashed"))
}),
caption: [The results from Table 1 visualized. The dashed line shows a linear regression from the data points. The regression has an intercept slightly above zero, though the slope seems a good fit for the data points, indicating a linear relationship.]
)

= Discussion and conclusion
#comment[Here you can discuss the outcome of the experiments and the experiences gained. Was your chosen approach suitable? What worked well and what could be improved?]

This experiment worked out well, based on my expectations. The scaling across a variety of worker service replicate numbers gave the expected performance results. The overhead of the system management was higher than expected, but it did not turn out to be bottleneck so this was no issue. A somewhat comedic observation was that the dimensionality reduction code spends most of its time downloading files from the object storage, not even performing the reduction.

All software choices I made at the beginning of the project also held up well. No component turned out to be unviable at any point. Wrapping the services in docker containers was also quite simple. The biggest issue there was resolving python dependencies properly, with some packages requiring undocumented build- or runtime dependencies.

The biggest issue was the python code that glues everything together. Having 3 python-based services communicate with each other over the network is really challening because there is zero type safety anywhere. I ended up putting the communication code into a common code base, which ended up more bloated than i wanted, but that made further development a bit easier (though still not foolproof).

The main two things that could clearly still be improved is 1) the docker image size and 2) error resilience. 1) Because so much code ended up best shared between services, even when there is only partial overlap in their function, pulled in a large number of implicit dependencies into images that have no use for them. 2) The downside of a system with this many components is that any component can fail at any time, and with nearly every task in the python code interacting with the object storage and MariaDB, there is a lot of potential for currently unhandled failure.

The code for this project is available on github #cite(<projectcode>).

= References
#comment[Be consistent in what format you use for your references. For example, use the MLA (Modern Language Association) format. At http://www.ub.uu.se/ you can click on “cite this item” and choose a format.]

#bibliography("bib.bib",style:"mla",title:none)
