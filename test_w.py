import wikipediaapi


def get_population(city_name):
    # Create a Wikipedia object
    wiki = wikipediaapi.Wikipedia('en')

    # Get the page for the city
    page = wiki.page(city_name)

    # Check if the page exists
    if page.exists():
        # Extract the section containing population information
        population_section = page.section_by_title('Demographics')

        # If the section doesn't exist, try another common section title
        if not population_section:
            population_section = page.section_by_title('Population')

        # If the section still doesn't exist, return None
        if not population_section:
            return None

        # Extract population information from the section
        population_info = population_section.text

        # Parse population from the text (this is a very basic parsing)
        population = None
        for line in population_info.split('\n'):
            if 'Population' in line:
                population = line.split(':')[1].strip()
                break

        return population
    else:
        return None


# Example usage
city_name = 'Yekaterinburg'
population = get_population(city_name)
print(f"Population of {city_name}: {population}")