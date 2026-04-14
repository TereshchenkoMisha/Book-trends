import React, { useState, useEffect, useRef } from 'react';
import '../App.css';

const Scrollytelling = () => {
  // --- State declarations ---
  // Tracks which iframes have finished loading (to show loader overlay)
  const [loadedGraphs, setLoadedGraphs] = useState({});
  // Stores indices of narrative text blocks that have become visible (for fade-in animation)
  const [visibleTexts, setVisibleTexts] = useState([]);
  // Stores indices of graph blocks that have become visible
  const [visibleGraphs, setVisibleGraphs] = useState([]);
  // Controls visibility of the first introductory text block (before any graph)
  const [visibleFirstText, setVisibleFirstText] = useState(false);
  // Controls visibility of the conclusion block (after all graphs)
  const [visibleConclusion, setVisibleConclusion] = useState(false);

  // --- Refs for IntersectionObserver targeting ---
  const textRefs = useRef([]);         // Holds DOM refs for narrative text blocks between graphs
  const graphRefs = useRef([]);        // Holds DOM refs for each graph block
  const firstTextRef = useRef(null);   // Ref for the first introductory text block
  const conclusionRef = useRef(null);  // Ref for the conclusion block

  // --- Data: graph definitions (URLs, titles, descriptions) ---
  const graphData = [
    {
      url: '/graphs/1_genre_cumulative_timeline.html',
      title: 'Cumulative Timeline by Genre',
      description: 'The stacked area chart shows how the number of books per genre accumulated from 1970 to 2020. Book production exploded from the 1990s onward – a possible digital publishing effect. Also in 2000, nonfiction overtook fiction in terms of the number of publications. The number of books published after 2000 in the dataset is small. This happened due to the peculiarity of the sources from which we parsed the data; most do not contain data on new books.'
    },
    {
      url: '/graphs/2_historical_events_figures.html',
      title: '️ Top Historical Events & Figures',
      description: 'World War II dominates the dataset with 1,786 books, followed by the World War I (593) and the Cold War (495). The American Civil War and Civil Rights Movement also appear high on the list.\n\nWhat it reflects:\nGlobal‑scale conflicts and transformative social movements generate the most literature. WWII alone accounts for more books than the next five events combined – it remains the West’s central historical reference point. The most mentioned historical figure is Abraham Lincoln (326), with more mentions than Hitler (274) and Jesus (268).'
    },
    {
      url: '/graphs/3_event_rating_popularity.html',
      title: 'Rating vs. Popularity (by Event)',
      description: 'The Holocaust has the highest average rating (3.87), though it appears in fewer books than WWII. The Civil Rights Movement and Vietnam War also earn high ratings, while the Cold War receives a lower score despite its many mentions.\n\nWhat it reflects:\nsome events, though less written about, are more deeply appreciated by readers. Quality and quantity do not always align – the Holocaust’s books tend to be highly rated, possibly because they are often profound memoirs or historical analyses.'
    },
    {
      url: '/graphs/4_rating_waterfall.html',
      title: 'Rating Waterfall by Decade',
      description: 'Compared to the 1970s baseline (3.87), average book ratings have slowly increased over time — reaching +0.067 in the 2020s. Modern books tend to be rated slightly higher. This rise likely reflects a mix of changing reader habits (online rating culture encourages higher scores) and improved publishing quality (better editing, design, and genre diversity).'
    },
    {
      url: '/graphs/5_genre_event_heatmap.html',
      title: 'Genre–Event Heatmap',
      description: "World War II is most tightly linked to the “Mystery and Thriller” (55.4%) and to “War” genre (53.4%). Romance and Fiction often mentioned WWII, but spread their mentions more evenly across events. The Civil War is also frequently mentioned in Adventure (32.4%) and Historical fiction (29.2%).\n\nWhat it reflects:\ngenre determines how history is framed. War and Historical Fiction dramatise conflicts; Mysteries use WWII as a backdrop for suspense; Biographies give voice to a wider range of events, from revolutions to civil rights."
    },
    {
      url: '/graphs/6_authors_event_lollipop.html',
      title: 'Top Authors Writing About Historical Events',
      description: 'The chart compares total books by the author against books specifically about historical event – for example Karl Marx about Industrial Revolution.\n\nWhat it reflects:\na small group of historians and memoirists shape how millions remember WWII. Their deep dives create authoritative canons, but also risk narrowing the narrative to a few celebrated voices.'
    },
    {
      url: '/graphs/7_civil_wars_choropleth.html',
      title: 'Choropleth: Civil Wars by Country',
      description: 'The United States dominates books about the American Civil War (1095 books). But also USA has a lot of books about civil wars of other countries. This is explained by active emigration to the United States in the 20th century and the probable availability of a larger number of books in English. The map shows how national history drives local publishing.\n\nIt is also important to note that books about the civil war have been published in almost half of the countries in the world.'
    }
  ];

  // --- Narrative transition texts placed between graphs ---
  const betweenTexts = [
    "🔍 But which specific historical events inspired the most books? Let’s move from genres to events.",
    "⭐ Popularity is one thing, but do readers rate these events equally? The next scatter plot compares rating with popularity.",
    "📊 Are books getting better over time, or are we simply rating them differently? The waterfall chart shows rating trends by decade.",
    "📉 But how do genres and events mix? The heatmap reveals which genres are most obsessed with which events.",
    "✒️ Some authors become so identified with a single event that they define our understanding of it. The lollipop chart shows who wrote most about what.",
    "🗺️ Finally, geography matters. Where do authors come from when writing about a specific civil war? The choropleth map answers."
  ];

  // --- Introductory text (first block) with inline styling for highlighted years ---
  const firstText = (
    <>
      We created and analysed a large dataset of books published between{' '}
      <span style={{ color: 'rebeccapurple', fontWeight: 'bold' }}>1970 and 2020</span>. Their genres, historical events, ratings, and authors. <span style={{ color: 'rebeccapurple', fontWeight: 'bold' }}>The goal was simple:</span> to uncover how we remember, interpret, and retell history through the books we write and read.
      <br /><br />
      <h2 className="scroll-hint">Scroll down to explore the story.</h2>
    </>
  );

  // --- Conclusion text (appears after all graphs) ---
  const conclusionText = (
    <>
        <h2 style={{ marginBottom: '1rem', fontFamily: 'MyCustomFontH', color: '#bbb', fontSize: '2rem' }}>📖 So, what do books tell about us?</h2>
        <p>
        ● Popularity ≠ appreciation. WWII is the most written‑about event, but the Holocaust receives higher
        ratings. <br/><br/>

        ● Genres filter history. War novels dramatise conflicts, mysteries use them as settings, and biographies spread
        across many events. <br/><br/>

         ●Ratings are rising slowly. Modern books score slightly higher than those from the 1970s – a subtle but
        persistent trend. <br/><br/>

        ● Authors become gatekeepers. A handful of writers define entire historical periods through their repeated focus.
        <br/><br/>

        ● History not always is local. Civil wars are mostly written by authors from the countries that fought them, but sometimes books about the Civil War may be published in another country.
        <br/><br/><p/>
        This project is a reminder: the books we choose to publish, read, and rate reveal what we value,
        what we mourn, and what we hope to never forget.
        <br/><br/>
        <h2 className="scroll-hint">Thank you for scrolling through the story.</h2>
      </p>
    </>
  );

  // --- Callback triggered when an iframe finishes loading ---
  const handleIframeLoad = (index) => {
    // Mark the specific graph as loaded to hide the loading overlay
    setLoadedGraphs(prev => ({ ...prev, [index]: true }));
  };

  // --- IntersectionObserver for the first text block (before first graph) ---
  // Triggers fade-in when block enters viewport
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) {
          setVisibleFirstText(true);
        }
      },
      { threshold: 0.3 } // 30% of element must be visible
    );
    if (firstTextRef.current) observer.observe(firstTextRef.current);
    return () => observer.disconnect();
  }, []);

  // --- IntersectionObserver for the conclusion block (after all graphs) ---
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) {
          setVisibleConclusion(true);
        }
      },
      { threshold: 0.3 }
    );
    if (conclusionRef.current) observer.observe(conclusionRef.current);
    return () => observer.disconnect();
  }, []);

  // --- IntersectionObserver for narrative text blocks between graphs ---
  // Adds the block's index to visibleTexts when it becomes visible
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          const index = entry.target.getAttribute('data-text-index');
          if (entry.isIntersecting && index !== null) {
            setVisibleTexts((prev) =>
              prev.includes(index) ? prev : [...prev, index]
            );
          }
        });
      },
      { threshold: 0.3 }
    );
    // Observe all narrative text refs
    textRefs.current.forEach(ref => ref && observer.observe(ref));
    return () => observer.disconnect();
  }, []);

  // --- IntersectionObserver for graph blocks ---
  // Adds the graph index to visibleGraphs when the graph block enters viewport
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          const index = entry.target.getAttribute('data-graph-index');
          if (entry.isIntersecting && index !== null) {
            setVisibleGraphs((prev) =>
              prev.includes(index) ? prev : [...prev, index]
            );
          }
        });
      },
      { threshold: 0.2 } // 20% visibility needed for graphs (slightly lower for smoother scroll)
    );
    graphRefs.current.forEach(ref => ref && observer.observe(ref));
    return () => observer.disconnect();
  }, []);

  // --- Render component ---
  return (
    <div className="container">
      {/* Title section - always visible */}
      <section className="intro">
        <h1>📚 Books Data Scrollytelling</h1>
        <h2>What books can tell about us?</h2>
      </section>

      <div className="graph-blocks">
        {/* First introductory text block (appears before first graph) */}
        <div
          ref={firstTextRef}
          className={`between-graphs-text ${visibleFirstText ? 'fade-in-visible' : 'fade-in-hidden'}`}
        >
          <p>{firstText}</p>
        </div>

        {/* Iterate over all graphs */}
        {graphData.map((item, idx) => (
          <React.Fragment key={idx}>
            {/* Graph block with iframe and description */}
            <div
              ref={(el) => (graphRefs.current[idx] = el)}
              data-graph-index={String(idx)}
              className={`graph-block ${visibleGraphs.includes(String(idx)) ? 'graph-fade-visible' : 'graph-fade-hidden'}`}
            >
              <div className="graph-wrapper">
                <iframe
                  src={item.url}
                  title={`Graph ${idx + 1}`}
                  onLoad={() => handleIframeLoad(idx)}
                  className="graph-iframe"
                />
                {/* Show loader overlay until iframe loads */}
                {!loadedGraphs[idx] && (
                  <div className="iframe-loader-overlay">
                    <div className="loader-content">
                      <div>⏳</div>
                      <div>Loading graph...</div>
                    </div>
                  </div>
                )}
              </div>
              <div className="text-block">
                <h2>{item.title}</h2>
                <p>{item.description}</p>
              </div>
            </div>

            {/* Narrative transition block between graphs (skip after last graph) */}
            {idx < betweenTexts.length && (
              <div
                ref={(el) => (textRefs.current[idx] = el)}
                data-text-index={String(idx)}
                className={`between-graphs-text ${visibleTexts.includes(String(idx)) ? 'fade-in-visible' : 'fade-in-hidden'}`}
              >
                <p>{betweenTexts[idx]}</p>
              </div>
            )}
          </React.Fragment>
        ))}

        {/* Conclusion block after all graphs */}
        <div
          ref={conclusionRef}
          className={`between-graphs-text ${visibleConclusion ? 'fade-in-visible' : 'fade-in-hidden'}`}
          style={{ marginBottom: '4rem' }}
        >
          <p>{conclusionText}</p>
        </div>
      </div>
    </div>
  );
};

export default Scrollytelling;