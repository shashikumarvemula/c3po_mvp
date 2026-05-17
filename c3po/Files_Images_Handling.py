from Structured_Bot.helper import FileNameExtractor
import chainlit as cl
import os
import pickle
import plotly.graph_objects as go
import time
class ElementsHandling:
    def __init__(self):
        pass

    def get_files_images(self,python_code,saved_files_lst):
        """
        This function is used to get the files and imges from the code provided
        """
        images_files_list = FileNameExtractor.extract_images_names(python_code)
        excel_files_list = FileNameExtractor.extract_excel_names(python_code)
        pkl_files_list = FileNameExtractor.extract_pkl_names(python_code)

        ppt_files_list = FileNameExtractor.extract_ppt_names(python_code)

        excel_files_list.extend(ppt_files_list)

        saved_files_lst.extend(images_files_list)
        saved_files_lst.extend(excel_files_list)

        return images_files_list,excel_files_list,ppt_files_list,pkl_files_list
    
    
    async def saving_files_to_ui(self,images_files_list):
        
        for i, image_file in enumerate(images_files_list):
            filename = image_file
            if os.path.exists(filename):
                print("image file exists")
                await cl.sleep(1)
                elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]
                await cl.Message(content=f"Chart:{i+1}", elements=elements).send()# #disable_feedback=False
                await cl.sleep(1)
                if os.path.exists(filename):
                    try:
                        print("removing image file: ", filename)
                        os.remove(filename)
                        print(f"{filename} has been deleted.")
                    except Exception as e:
                        print(f"Error while deleting {filename}: {str(e)}")
                else:
                    print(f"{filename} does not exist.")
                    
    async def excel_file_handler(self,excel_files_list):
        for i, excel_file in enumerate(excel_files_list):
            filename = excel_file
            if os.path.exists(filename):
                print("excel file exists")
                #elements = [cl.Image(name="image_"+str(time.time()), display="inline",size = "large", path=f"./{filename}")]
                elements = [cl.File(name= filename, path=f"./{filename}",display="inline",),]
                try:
                    await cl.Message(content=f"File-{i+1}: {excel_file}", elements=elements).send()
                    await cl.sleep(0.5)
                except Exception as e:
                    print("Exception occured at 907 and e: ", e)

    async def pptx_file_handler(self, pptx_files_list):
        """
        Handle PPTX files and send them to UI
        
        Args:
            pptx_files_list: List of PPTX file paths
        """
        for i, pptx_file in enumerate(pptx_files_list):
            filename = pptx_file
            if os.path.exists(filename):
                print("PPTX file exists")
                
                # Create file element for PPTX
                elements = [cl.File(name=filename,path=f"./{filename}",display="inline"),]
                try:
                    await cl.Message(
                        content=f"📊 PowerPoint Presentation {i+1}: {os.path.basename(filename)}", 
                        elements=elements
                    ).send()
                    await cl.sleep(0.5)
                    print(f"PPTX file {filename} sent to UI successfully")
                except Exception as e:
                    print(f"Exception occurred while sending PPTX file: {e}")
            else:
                print(f"PPTX file {filename} does not exist")
    

    async def pkl_file_handler(self, pkl_files_list):
        """
        Handle PKL files (Plotly figures) and display them as interactive charts
        
        Args:
            pkl_files_list: List of PKL file paths containing Plotly figures
        """
        for i, pkl_file in enumerate(pkl_files_list):
            # Get full path to the file in user directory
            # pkl_file = self.get_user_file_path(pkl_file)

            if os.path.exists(pkl_file):
                print(f"PKL file exists: {pkl_file}")
                
                try:
                    # Read the pickled Plotly figure from directory
                    with open(pkl_file, 'rb') as f:
                        plotly_figure = pickle.load(f)
                        
                    
                    plotly_figure.update_layout(width=1400) 
                    
                    # Verify it's a valid Plotly figure
                    if isinstance(plotly_figure, go.Figure):
                        # Display interactive Plotly chart
                        elements = [cl.Plotly(
                            name=f"interactive_chart_{i+1}_{time.time()}", 
                            figure=plotly_figure,
                            size="large",
                            display="inline"
                        )]
                        
                        await cl.Message(
                            content=f"📊 Chart {i+1}: {pkl_file}", 
                            elements=elements
                        ).send()
                        await cl.sleep(0.5)
                        print(f"PKL file {pkl_file} displayed as interactive chart successfully")
                        
                        # Delete the file after displaying (optional - follow your existing pattern)
                        if os.path.exists(pkl_file):
                            try:
                                os.remove(pkl_file)
                                print(f"{pkl_file} has been deleted.")
                            except Exception as e:
                                print(f"Error while deleting {pkl_file}: {str(e)}")
                    
                    else:
                        print(f"PKL file {pkl_file} does not contain a valid Plotly figure")
                        # Fallback: send as downloadable file
                        elements = [cl.File(
                            name=pkl_file,
                            path=pkl_file,
                            display="inline"
                        )]
                        await cl.Message(
                            content=f"📄 Data File {i+1}: {pkl_file}", 
                            elements=elements
                        ).send()
                        
                except Exception as e:
                    print(f"Exception occurred while processing PKL file {pkl_file}: {e}")
                    # Fallback: send as downloadable file
                    try:
                        elements = [cl.File(
                            name=pkl_file,
                            path=pkl_file,
                            display="inline"
                        )]
                        await cl.Message(
                            content=f"📄 Data File {i+1}: {pkl_file}", 
                            elements=elements
                        ).send()
                    except Exception as fallback_error:
                        print(f"Fallback also failed for {pkl_file}: {fallback_error}")
            else:
                print(f"PKL file {pkl_file} does not exist")
    
    async def remove_content_and_saved_files(self,content_ui_message, saved_files_lst):
        if content_ui_message:
            content_ui_message.remove()
        for filename in saved_files_lst:
            if os.path.exists(filename):
                try:
                    os.remove(filename)
                    print(f"{filename} has been deleted.")
                except Exception as e:
                    print(f"Error while deleting {filename}: {str(e)}")
    