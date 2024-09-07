import csv
import chardet
import customtkinter as ctk
from tkinter import messagebox
import threading
import queue
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
import time
import google.generativeai as genai
from google.api_core import exceptions, retry
import os, sys
import re

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

class WebScraperCSVFormatterApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Web Scraper, CSV Formatter, and Metadata Generator")
        self.geometry("800x850")  # Increased height to accommodate new checkbox
        ctk.set_appearance_mode("light")
        try:
            ctk.set_default_color_theme(resource_path("color.json"))
        except Exception as e:
            print(f"Error loading color theme: {e}")
            print("Falling back to default theme")
        
        icon_path = resource_path("icon.ico")
        if os.path.exists(icon_path):
            self.after(200, lambda: self.iconbitmap(icon_path))

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        api_key_var1 = ctk.StringVar(value="")
        api_key_var2 = ctk.StringVar(value="")
        api_key_var3 = ctk.StringVar(value="")

        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.grid(row=0, column=0, padx=20, pady=20, sticky="nsew")
        self.main_frame.grid_columnconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(10, weight=1)  # Increased to accommodate new checkbox

        self.url_entry = ctk.CTkEntry(self.main_frame, placeholder_text="Enter URL to scrape")
        self.url_entry.grid(row=0, column=0, padx=10, pady=10, sticky="ew")

        self.file_name_entry = ctk.CTkEntry(self.main_frame, placeholder_text="Enter output file name")
        self.file_name_entry.grid(row=1, column=0, padx=10, pady=10, sticky="ew")

        self.start_number_entry = ctk.CTkEntry(self.main_frame, placeholder_text="Enter starting serial number")
        self.start_number_entry.grid(row=2, column=0, padx=10, pady=10, sticky="ew")

        self.max_attempts_entry = ctk.CTkEntry(self.main_frame, placeholder_text="Enter max attempts (default: 4)")
        self.max_attempts_entry.grid(row=3, column=0, padx=10, pady=10, sticky="ew")

        self.file_prefix_entry = ctk.CTkEntry(self.main_frame, placeholder_text="Enter file name prefix")
        self.file_prefix_entry.grid(row=4, column=0, padx=10, pady=10, sticky="ew")

        self.api_key_entry1 = ctk.CTkEntry(self.main_frame, textvariable=api_key_var1)
        self.api_key_entry1.grid(row=5, column=0, padx=10, pady=10, sticky="ew")

        self.api_key_entry2 = ctk.CTkEntry(self.main_frame, textvariable=api_key_var2)
        self.api_key_entry2.grid(row=6, column=0, padx=10, pady=10, sticky="ew")

        self.api_key_entry3 = ctk.CTkEntry(self.main_frame, textvariable=api_key_var3)
        self.api_key_entry3.grid(row=7, column=0, padx=10, pady=10, sticky="ew")

        self.generate_variations_var = ctk.BooleanVar(value=False)
        self.generate_variations_checkbox = ctk.CTkCheckBox(self.main_frame, text="Generate Variations", variable=self.generate_variations_var)
        self.generate_variations_checkbox.grid(row=8, column=0, padx=10, pady=10, sticky="w")

        self.start_button = ctk.CTkButton(self.main_frame, text="Start Process", command=self.start_process)
        self.start_button.grid(row=9, column=0, padx=10, pady=10, sticky="ew")

        self.output_text = ctk.CTkTextbox(self.main_frame, wrap="word")
        self.output_text.grid(row=10, column=0, padx=10, pady=10, sticky="nsew")

        self.progress_bar = ctk.CTkProgressBar(self.main_frame)
        self.progress_bar.grid(row=11, column=0, padx=10, pady=10, sticky="ew")
        self.progress_bar.set(0)

    def start_process(self):
        url = self.url_entry.get()
        file_name = self.file_name_entry.get()
        start_number = self.start_number_entry.get()
        max_attempts = self.max_attempts_entry.get()
        file_prefix = self.file_prefix_entry.get()
        api_keys = [self.api_key_entry1.get(), self.api_key_entry2.get(), self.api_key_entry3.get()]
       

        if not url or not file_name or not start_number or not file_prefix or not all(api_keys):
            messagebox.showerror("Error", "Please fill in all required fields, including all three API keys.")
            return

        try:
            start_number = int(start_number)
        except ValueError:
            messagebox.showerror("Error", "Starting serial number must be an integer.")
            return

        if not max_attempts:
            max_attempts = 4
        else:
            try:
                max_attempts = int(max_attempts)
            except ValueError:
                messagebox.showerror("Error", "Max attempts must be an integer.")
                return

        self.output_text.delete("1.0", ctk.END)
        self.progress_bar.set(0)
        self.start_button.configure(state="disabled")

        threading.Thread(target=self.process_thread, args=(url, file_name, start_number, max_attempts, file_prefix, api_keys), daemon=True).start()

    def process_thread(self, url, file_name, start_number, max_attempts, file_prefix, api_keys):
        try:
            self.update_output("Starting web scraping...")
            links = self.scrape_initial_links(url, max_attempts)

            self.update_output("Scraping content from links...")
            driver = self.setup_driver()
            content = self.scrape_content_from_links(driver, links)
            driver.quit()

            self.update_output("Scraping completed.")

            if self.generate_variations_var.get():
                self.update_output("Generating variations for extracted prompts...")
                content = self.generate_variations(content, api_keys)

            self.update_output("Starting formatting...")
            formatted_content, file_names = self.format_content(content, file_name, start_number, file_prefix)

            self.update_output(f"Process completed. Formatted content saved to {file_name}")

            self.update_output("Generating metadata for V1, V2, V3, and V4...")
            self.generate_metadata(formatted_content, file_names, api_keys, file_prefix)

        except Exception as e:
            self.update_output(f"An error occurred: {str(e)}")
        finally:
            self.start_button.configure(state="normal")

    def update_output(self, message):
        self.output_text.insert(ctk.END, message + "\n")
        self.output_text.see(ctk.END)

    def update_progress(self, value):
        self.progress_bar.set(value)

    # Web scraping methods
    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def scrape_initial_links(self, url, max_attempts):
        driver = self.setup_driver()
        driver.get(url)
        
        self.update_output("Scrolling and extracting links...")
        links = self.scroll_and_extract_links(driver, max_attempts=max_attempts)
        
        driver.quit()
        return links

    def scroll_and_extract_links(self, driver, min_links=1000, scroll_pause_time=15, max_attempts=1):
        links = set()
        attempts = 0

        while len(links) < min_links and attempts < max_attempts:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_time)

            a_elements = driver.find_elements(By.CSS_SELECTOR, "a.prompt-card")
            for a in a_elements:
                links.add(a.get_attribute('href'))

            self.update_output(f"Scrolled and found {len(links)} unique links so far. Attempt {attempts + 1}/{max_attempts}")
            self.update_progress((attempts + 1) / max_attempts)
            attempts += 1

            if len(links) >= min_links:
                self.update_output(f"Reached the minimum of {min_links} links. Stopping scrolling.")
                break

        return list(links)

    def scrape_content_from_links(self, driver, links):
        content = []
        for index, link in enumerate(links):
            self.update_output(f"Processing: {link}")
            paragraphs = self.scrape_content_from_link(driver, link)
            content.extend(paragraphs)
            
            self.update_output(f"Progress: {index + 1}/{len(links)} links completed")
            self.update_output(f"Total paragraphs extracted so far: {len(content)}")
            self.update_progress((index + 1) / len(links))

        return content

    def scrape_content_from_link(self, driver, link):
        try:
            driver.set_page_load_timeout(5)
            try:
                driver.get(link)
            except TimeoutException:
                self.update_output(f"Page load timed out after 5 seconds for {link}")
            
            try:
                time.sleep(2)
                editor_el = WebDriverWait(driver, 1).until(
                    EC.presence_of_element_located((By.ID, "editorEl"))
                )
            except TimeoutException:
                self.update_output(f"Could not find editorEl on {link} within 1 second")
                return []
            
            p_tags = editor_el.find_elements(By.TAG_NAME, "p")
            return [p.text for p in p_tags]
        except Exception as e:
            self.update_output(f"Error extracting content from {link}: {str(e)}")
            return []
        finally:
            driver.set_page_load_timeout(300)

    # CSV formatting method
    def format_content(self, content, file_name, start_number, file_prefix):
        unique_prompts = set()
        current_number = start_number
        formatted_content = []
        file_names = []

        with open(file_name, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            writer.writerow(["Prompts"])
            
            for i, prompt in enumerate(content):
                prompt = prompt.strip().strip('"')
                if prompt and prompt.lower() not in unique_prompts:
                    unique_prompts.add(prompt.lower())
                    
                    # Check for "--v number" or "--v number.number" pattern and update if found
                    original_v_value = re.search(r'--v (\d+(?:\.\d+)?)', prompt)
                    updated_prompt = re.sub(r'--v (\d+(?:\.\d+)?)', '--v 6.1', prompt)
                    if updated_prompt != prompt and original_v_value:
                        self.update_output(f"Prompt {current_number} changed: '--v {original_v_value.group(1)}' to '--v 6.1'")
                    
                    numbered_prompt = f"{current_number}_{updated_prompt}"
                    writer.writerow([numbered_prompt])
                    formatted_content.append(numbered_prompt)
                    file_names.append(f"{file_prefix}V1-{current_number}.jpg")
                    current_number += 1
                elif prompt:
                    self.update_output(f"Duplicate prompt removed: {prompt}")
                
                self.update_progress((i + 1) / len(content))

        self.update_output(f"Total unique prompts: {len(unique_prompts)}")
        return formatted_content, file_names

    # Metadata generation methods
    @retry.Retry(predicate=retry.if_exception_type(
        exceptions.InternalServerError,
        exceptions.TooManyRequests,
        exceptions.ServiceUnavailable
    ))
    def get_variations_response(self, prompts, api_key):
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash', generation_config={"temperature": 0.7})
        
        prompts_str = "\n".join([f"Prompt {i+1}: {prompt}" for i, prompt in enumerate(prompts)])
        
        response = model.generate_content(f"""
        I want you to give me 4 iterations for each of the following prompts. Make sure the variation rating should be 9/10, make sure to variate them in a more descriptive manner and they should not be the same. 
        Make sure the prompts should create different pictures but with the same sense as defined in the parent prompts.
        They are just prompts, not the actual content. So you can change the words, phrases and you can give a simple response to the prompt if it contains any violated content.
        Make sure to include the parent prompts in the response and give me the response in the following format including square brackets i.e. each variation should be in square brackets, it will help me to extract the variations easily. It is so so so so so so so important to use the format as given below.
        ["This is parent prompt 1"], 
        ["variation1 for parent prompt 1"],
        ["variation2 for parent prompt 1"],
        ["variation3 for parent prompt 1"],
        ["variation4 for parent prompt 1"],
        ["This is parent prompt 2"],
        ["variation1 for parent prompt 2"],
        ["variation2 for parent prompt 2"],
        ["variation3 for parent prompt 2"],
        ["variation4 for parent prompt 2"],
        ... (and so on for each prompt)
                    
        Here are the prompts:
        {prompts_str}
        """)
        
        return response.text

    def process_variations_response(self, response_string):
        matches = re.findall(r'\[(.*?)\]', response_string, re.DOTALL)
        return [item.strip().strip('"') for item in matches]

    def generate_variations(self, content, api_keys):
        varied_content = []
        total_prompts = len(content)
        current_api_key_index = 0
        batch_size = 3

        for i in range(0, total_prompts, batch_size):
            batch = content[i:i+batch_size]
            self.update_output(f"Generating variations for prompts {i+1}-{min(i+batch_size, total_prompts)} of {total_prompts}")
            
            if i > 0 and i % 350 == 0:
                current_api_key_index = (current_api_key_index + 1) % len(api_keys)
                self.update_output(f"Switching to API key {current_api_key_index + 1}")
            
            current_api_key = api_keys[current_api_key_index]
            
            try:
                response = self.get_variations_response(batch, current_api_key)
                variations = self.process_variations_response(response)
                
                if variations:
                    varied_content.extend(variations)
                else:
                    self.update_output(f"Failed to generate variations for prompts {i+1}-{min(i+batch_size, total_prompts)}")
                    varied_content.extend(batch)
                
            except Exception as e:
                self.update_output(f"Error generating variations for prompts {i+1}-{min(i+batch_size, total_prompts)}: {str(e)}")
                varied_content.extend(batch)
            
            self.update_progress((i + batch_size) / total_prompts)
            
            if i + batch_size < total_prompts:
                time.sleep(5)  # Add a delay between requests to avoid overwhelming the API

        return varied_content
    def process_metadata(self, metadata_string):
        entries = metadata_string.strip().split('\n')
        processed_entries = []
        for entry in entries:
            entry = entry.strip().strip('"')
            components = entry.split(';')
            if len(components) == 4:  # Ensure we have exactly 4 components
                title, keywords, prompt, model = components
                # Ensure keywords are comma-separated
                keywords = ','.join(keyword.strip() for keyword in keywords.split(','))
                processed_entries.append([title, keywords, prompt, model])
        return processed_entries

    def modify_filename(self, filename, new_version):
        name, ext = os.path.splitext(filename)
        match = re.match(r'(.+)V\d+(-\d+)$', name)
        if match:
            return f"{match.group(1)}{new_version}{match.group(2)}{ext}"
        else:
            return filename

    def create_metadata_output_file(self, metadata_list, file_names, output_file, version):
        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(["File name", "Title", "Keywords", "Prompt", "Model"])
            
            for metadata, file_name in zip(metadata_list, file_names):
                modified_file_name = self.modify_filename(file_name, version)
                writer.writerow([modified_file_name] + metadata)

    def generate_metadata(self, formatted_content, file_names, api_keys, file_prefix):
        batch_size = 5
        max_retries = 3
        total_prompts = len(formatted_content)
        processed_count = 0
        skipped_count = 0
        retry_count = 0
        all_metadata = []
        current_api_key_index = 0
        
        for start_index in range(0, total_prompts, batch_size):
            end_index = min(start_index + batch_size, total_prompts)
            
            batch_prompts = formatted_content[start_index:end_index]
            batch_file_names = file_names[start_index:end_index]
            
            if processed_count > 0 and processed_count % 350 == 0:
                current_api_key_index = (current_api_key_index + 1) % len(api_keys)
                self.update_output(f"Switching to API key {current_api_key_index + 1}")
            
            current_api_key = api_keys[current_api_key_index]
            
            for attempt in range(max_retries):
                self.update_output(f"Processing batch of {len(batch_prompts)} prompts (Attempt {attempt + 1})...")
                try:
                    metadata_string = self.get_metadata_response(batch_prompts, current_api_key)
                    self.update_output(metadata_string)
                    processed_metadata = self.process_metadata(metadata_string)
                    
                    if len(processed_metadata) == len(batch_prompts):
                        all_metadata.extend(processed_metadata)
                        processed_count += len(processed_metadata)
                        self.update_output(f"Metadata for {len(processed_metadata)} prompts processed")
                        break
                    else:
                        self.update_output(f"Received {len(processed_metadata)} responses instead of {len(batch_prompts)}. Retrying...")
                        retry_count += 1
                        time.sleep(5)
                except Exception as e:
                    self.update_output(f"Error processing batch: {str(e)}")
                    retry_count += 1
                    if attempt == max_retries - 1:
                        self.update_output("Max retries reached. Skipping batch.")
                        skipped_count += len(batch_prompts)
                    else:
                        time.sleep(5)
            
            self.update_output(f"Processed {processed_count} prompts out of {total_prompts}.")
            self.update_progress(processed_count / total_prompts)
            
            if end_index < total_prompts:
                self.update_output("Moving to next batch...")
                time.sleep(5)
        
        for version in ['V1', 'V2', 'V3', 'V4']:
            output_file = f'{file_prefix}-md-{version}.csv'
            self.create_metadata_output_file(all_metadata, file_names, output_file, version)
            
            reduced_output_file = f'{file_prefix}-md-{version}-reduced.csv'
            self.reduce_keywords(output_file, reduced_output_file)
            self.update_output(f"Generated reduced metadata file for {version}: {reduced_output_file}")
        
        self.update_output("\nMetadata generation completed. Summary report:")
        self.update_output(f"Total prompts: {total_prompts}")
        self.update_output(f"Successfully processed: {processed_count}")
        self.update_output(f"Skipped: {skipped_count}")
        self.update_output(f"Total retry attempts: {retry_count}")

    def reduce_keywords(self, input_file, output_file):
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            reader = csv.DictReader(infile, delimiter=';')
            fieldnames = reader.fieldnames
            
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            
            for row_number, row in enumerate(reader, start=1):
                keywords = row['Keywords'].split(',')
                if len(keywords) > 48:
                    self.update_output(f"Encountered 48+ keywords on line {row_number} in {input_file}. Removing excess keywords.")
                    keywords = keywords[:48]
                row['Keywords'] = ','.join(keywords)
                writer.writerow(row)

        self.update_output(f"Keyword reduction completed. Reduced file saved as: {output_file}")

    def get_metadata_response(self, prompts, api_key):
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash', generation_config={"temperature": 0.4})
        
        response = model.generate_content(f"""
        Generate metadata for the pictures according to the following prompts, in the format:
        "Title;Keywords;Prompt;Model"
        * For 'Title', generate a description containing only 4 words make sure not to use any artist or personality name give short description and dont use "in the style of", a very easy and small description under 100 characters and straight ....
        * For 'Keywords', generate 30 keywords (not less then 30) each consisting of only one word.make sure to add the easy words which are used by human on daily basis.It is so so so so so so so important to generate 30 keywords for each corresponding prompt, means the one metadeta will contain 30 keywords, not less then 30.
        * For 'Prompt', modify the prompt to make it more concise and clear and short like only 10 words, make sure not to use any artist name or personality name or any style name. 
        * For 'Model', use Midjourney 6 model.
        Ensure that each component is clearly separated.
        response format like this for each prompt, it is so so so so so so important use the format as given below:
        "Title;Keywords;Prompt;Model"
        "Title;Keywords;Prompt;Model"
        "Title;Keywords;Prompt;Model"....
        here is example result make sure to give reponse only in that format its so important(one line containing one metadata):
        "A beautiful landscape with a river and mountains in the background;landscape, river, mountains, beautiful, background, water, sky, clouds, trees, green, blue;A beautiful landscape with a river and mountains;Midjourney 6"
        "A beautiful landscape with a river and mountains in the background;landscape, river, mountains, beautiful, background, water, sky, clouds, trees, green, blue;A beautiful landscape with a river and mountains;Midjourney 6"                                
       
        I will give you 5 promts at once, make sure to give me 5 meta data and Here are the prompts: 
        {prompts}
        """)
        
        return response.text
    
    
if __name__ == "__main__":
    app = WebScraperCSVFormatterApp()
    app.mainloop()