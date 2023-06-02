package proyectomundial;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.WindowConstants;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import proyectomundial.DAO.SeleccionDAO;
import proyectomundial.DAO.SeleccionDAO.EquipoConGoles;
import proyectomundial.model.Seleccion;

public class GUIManual extends JFrame {

    SeleccionDAO seleccionDAO = new SeleccionDAO();

    // Matrix que permite almancenar la información de las selecciones futbol cargadas
    public String[][] selecciones = null;

    // Matriz que permite almacenar los resultados de los partidos cargardos
    public String[][] resultados = null;

    // Elementos de bara Lateral
    private JPanel jPanelLeft;

    private JPanel jPanelIconFIFA;
    private JLabel iconFIFA;

    // Elementos de opciones de Menú
    private JPanel jPanelMenu;

    private JPanel jPanelMenuHome;
    private JLabel btnHome;

    private JPanel jPanelMenuSelecciones;
    private JLabel btnSelecciones;

    private JPanel jPanelMenuResultados;
    private JLabel btnResultados;

    private JPanel jPanelMenuDashboardSel;
    private JLabel btnDashboardSel;

    private JPanel jPanelMenuDashboardRes;
    private JLabel btnDashboardRes;

    // Elementos de panel de contenido
    private JPanel jPanelRight;
    private JPanel jPanelLabelTop;
    private JLabel jLabelTop;

    private JPanel jPanelMain;

    public GUIManual() {

        // Se inician los componentes gráficos
        initComponents();

        // Se configuran propiedades de nuestra Ventana
        setLocationRelativeTo(null);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setResizable(false);

        // Se llama la función home para que al momento de iniciar la aplicacoón, por defecto se muestre el home
        accionHome();

    }

    private void initComponents() {

        // Inicializamos componentes del Menu Lateral
        jPanelLeft = new JPanel();

        jPanelIconFIFA = new JPanel();
        iconFIFA = new JLabel();
        jPanelMenu = new JPanel();

        jPanelMenuHome = new JPanel();
        btnHome = new JLabel();

        jPanelMenuSelecciones = new JPanel();
        btnSelecciones = new JLabel();

        jPanelMenuResultados = new JPanel();
        btnResultados = new JLabel();

        jPanelMenuDashboardSel = new JPanel();
        btnDashboardSel = new JLabel();

        jPanelMenuDashboardRes = new JPanel();
        btnDashboardRes = new JLabel();

        // Pinta el logo de la aplicación
        pintarLogo();

        // Pinta la opción de menú del Home
        pintarMenuHome();

        // Pinta la opción de Menú de las Selecciones
        pintarMenuSelecciones();

        // Pinta la opción de Menú de los resultados
        pintarMenuResultados();

        // Pinta la opción de Menú del dashboard de equipo
        pintarMenuDashboardSel();

        // Pinta la opción de Menú del dahboard de resultados
        pintarMenuDashboardRes();

        // Pinta y ajuste diseño del contenedor del panel izquierdo
        pintarPanelIzquierdo();

        // Inicializa los componentes del panel derecho de los contenidos
        jPanelRight = new JPanel();
        jPanelLabelTop = new JPanel();
        jPanelMain = new JPanel();

        // Pinta la barra superrior de color azul claro, del panel de contenido
        pintarLabelTop();

        // Pinta y ajusta diseño del contenedor de contenidos
        pintarPanelDerecho();

        setTitle("Mundial");
        pack();
        setVisible(true);
    }

    private void pintarLogo() {
        jPanelIconFIFA.add(iconFIFA);
        jPanelIconFIFA.setOpaque(false);
        jPanelIconFIFA.setPreferredSize((new java.awt.Dimension(220, 80)));
        jPanelIconFIFA.setMaximumSize(jPanelIconFIFA.getPreferredSize());
        iconFIFA.setIcon(new ImageIcon(getClass().getResource("/resources/Easports_fifa_logo.svg.png")));
        jPanelLeft.add(jPanelIconFIFA, BorderLayout.LINE_START);

    }

    /**
     * Función que se encarga de ajustar los elementos gráficos que componente
     * la opción de navegación del HOME Define estilos, etiquetas, iconos que
     * decoran la opción del Menú. Esta opción de Menu permite mostrar la página
     * de bienvenida de la aplicación
     */
    private void pintarMenuHome() {
        btnHome.setIcon(new ImageIcon(getClass().getResource("/resources/icons/home.png"))); // NOI18N
        btnHome.setText("Home");
        btnHome.setForeground(new java.awt.Color(255, 255, 255));

        JLabel vacioHome = new JLabel();
        jPanelMenuHome.setBackground(new java.awt.Color(17, 41, 63));
        jPanelMenuHome.setPreferredSize((new java.awt.Dimension(220, 35)));
        jPanelMenuHome.setLayout(new BorderLayout(15, 0));
        jPanelMenuHome.add(vacioHome, BorderLayout.WEST);
        jPanelMenuHome.add(btnHome, BorderLayout.CENTER);
        jPanelMenu.add(jPanelMenuHome);

        btnHome.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                System.out.println("Home");
                accionHome();
            }
        });
    }

    /**
     * Función que se ejecuta cuando el usuario hacer click sobre la opción de
     * navegación Home Permite modificar la etiqueta de Navegación en Home,
     * remover los elementos que hay en el panel de contenidos y agregar la
     * imagen de inicio de la aplicación
     */
    private void accionHome() {
        jLabelTop.setText("Home");//cambiar el texto 
        //jLabelTopDescription.setText("Bievenido al sistema de gestión de mundiales de fútbol");

        jPanelMain.removeAll();
        JPanel homePanel = new JPanel();
        JLabel imageHome = new JLabel();

        imageHome.setIcon(new ImageIcon(getClass().getResource("/resources/home.jpg"))); // NOI18N
        //imageHome.setPreferredSize(new java.awt.Dimension(810, 465));
        homePanel.add(imageHome);

        jPanelMain.add(homePanel, BorderLayout.CENTER);
        jPanelMain.repaint();
        jPanelMain.revalidate();
    }

    /**
     * Función que se encarga de ajustar los elementos gráficos que componente
     * la opción de navegación de SELECCIONES Define estilos, etiquetas, iconos
     * que decoran la opción del Menú. Esta opción de Menu permite mostrar las
     * selecciones de futbol cargadas en la aplicación
     */
    private void pintarMenuSelecciones() {
        btnSelecciones.setIcon(new ImageIcon(getClass().getResource("/resources/icons/selecciones.png"))); // NOI18N
        btnSelecciones.setText("Selecciones");
        btnSelecciones.setForeground(new java.awt.Color(255, 255, 255));

        JLabel vacioSelecciones = new JLabel();
        jPanelMenuSelecciones.setBackground(new java.awt.Color(17, 41, 63));
        jPanelMenuSelecciones.setPreferredSize((new java.awt.Dimension(220, 35)));
        jPanelMenuSelecciones.setLayout(new BorderLayout(15, 0));
        jPanelMenuSelecciones.add(vacioSelecciones, BorderLayout.WEST);
        jPanelMenuSelecciones.add(btnSelecciones, BorderLayout.CENTER);
        jPanelMenu.add(jPanelMenuSelecciones);

        btnSelecciones.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                System.out.println("Selecciones");
                accionSelecciones();
            }
        });
    }

    /**
     * Función que se ejecuta cuando el usuario hace click sobre la opción de
     * navegación Selecciones Permite ver la lista de selecciones que se
     * encuentran cargadas en la aplicación. Si la lista de selecciones en
     * vacía, muestra un botón que permite cargar un archivo CSV con la
     * información de las selelecciones
     */
    private void accionSelecciones() {
        jLabelTop.setText("Selecciones");
        selecciones = seleccionDAO.getSeleccionesMatriz();

        // Si no hay selecciones cargadas, muestra el botón de carga de selecciones
        if (selecciones == null) {
            jPanelMain.removeAll();
            JPanel seleccionesPanel = new JPanel();

            JLabel notSelecciones = new JLabel();
            notSelecciones.setText("No hay selecciones cargadas, por favor cargue selecciones \n\n");
            seleccionesPanel.add(notSelecciones);

            JButton cargarFile = new JButton();
            cargarFile.setText("Seleccione el archivo");
            seleccionesPanel.add(cargarFile);
            cargarFile.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    cargarFileSelecciones();
                }
            });

            jPanelMain.add(seleccionesPanel);
            jPanelMain.repaint();
            jPanelMain.revalidate();
        } // Si hay selecciones cargadas, llama el método que permite pintar la tabla de selecciones
        else {
            pintarTablaSelecciones();
        }
    }

    /**
     * Función que se encarga de ajustar los elementos gráficos que componente
     * la opción de navegación de RESULTADOS Define estilos, etiquetas, iconos
     * que decoran la opción del Menú. Esta opción de Menu permite mostrar los
     * diferentes resultados de los partidos de la fase de grupos de un mundial
     */
    private void pintarMenuResultados() {
        btnResultados.setIcon(new ImageIcon(getClass().getResource("/resources/icons/resultados.png"))); // NOI18N
        btnResultados.setText("Resultados");
        btnResultados.setForeground(new java.awt.Color(255, 255, 255));

        JLabel vacioResultados = new JLabel();
        jPanelMenuResultados.setBackground(new java.awt.Color(17, 41, 63));
        jPanelMenuResultados.setPreferredSize((new java.awt.Dimension(220, 35)));
        jPanelMenuResultados.setLayout(new BorderLayout(15, 0));
        jPanelMenuResultados.add(vacioResultados, BorderLayout.WEST);
        jPanelMenuResultados.add(btnResultados, BorderLayout.CENTER);
        jPanelMenu.add(jPanelMenuResultados);

        btnResultados.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                accionResultados();
            }
        });
    }

    /**
     * Función que se ejecuta cuando el usuario hace click sobre la opción de
     * navegación Resultados Permite ver la lista de resultados que se
     * encuentran cargadas en la aplicación. Si la lista de resultados en vacía,
     * muestra un botón que permite cargar un archivo CSV con la información de
     * los resultados
     */
    private void accionResultados() {
        jLabelTop.setText("Resultados");

        // Si no hay resultados cargados, muestra el botón de carga de resultados
        if (resultados == null) {
            jPanelMain.removeAll();
            JPanel resultadosPanel = new JPanel();

            if (resultados == null) {

                JLabel notResultados = new JLabel();
                notResultados.setText("No hay resultados, por favor cargue resultados \n\n");
                resultadosPanel.add(notResultados);

                JButton cargarFile = new JButton();
                cargarFile.setText("Seleccione el archivo");
                resultadosPanel.add(cargarFile);
                cargarFile.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent evt) {
                        cargarFileResultados();
                    }
                });
            }

            jPanelMain.add(resultadosPanel);
            jPanelMain.repaint();
            jPanelMain.revalidate();
        } // Si hay ressultados cargados, llama el método que permite pintar la tabla de resultados
        else {
            pintarTablaResultados();
        }
    }

    /**
     * Función que se encarga de ajustar los elementos gráficos que componente
     * la opción de navegación de Dashboard de Selecciones Define estilos,
     * etiquetas, iconos que decoran la opción del Menú. Esta opción de Menu
     * permite mostrar los diferentes datos que será extraidos de la información
     * de las selecciones de futbol que fueron cargadas
     */
    private void pintarMenuDashboardSel() {
        btnDashboardSel.setIcon(new ImageIcon(getClass().getResource("/resources/icons/dashboard_selecciones.png")));
        btnDashboardSel.setText("Dash Selecciones");
        btnDashboardSel.setForeground(new java.awt.Color(255, 255, 255));

        JLabel vacioDashboardSelecciones = new JLabel();
        jPanelMenuDashboardSel.setBackground(new java.awt.Color(17, 41, 63));
        jPanelMenuDashboardSel.setPreferredSize((new java.awt.Dimension(220, 35)));
        jPanelMenuDashboardSel.setLayout(new BorderLayout(15, 0));
        jPanelMenuDashboardSel.add(vacioDashboardSelecciones, BorderLayout.WEST);
        jPanelMenuDashboardSel.add(btnDashboardSel, BorderLayout.CENTER);
        jPanelMenu.add(jPanelMenuDashboardSel);

        btnDashboardSel.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                System.out.println("Dashboard Selecciones");
                accionDashboardSel();
            }
        });
    }

    /**
     * TRABAJO DEL ESTUDIANTE Se debe módificar este método para poder calcular
     * y pintar las diferentes informaciones que son solicitadas Revise el
     * proceso que se siguen en los demás métodos para poder actualizar la
     * información de los paneles
     */
    private void accionDashboardSel() {
    jLabelTop.setText("Dash De Seleccion");

    // Obtener datos
    int totalSelecciones = seleccionDAO.getTotalSeleccionesCargadas();
    String[][] seleccionesPorContinente = seleccionDAO.getNumeroSeleccionesPorContinenteMatriz();
    int cantidadNacionalidadesDT = seleccionDAO.getCantidadNacionalidadesDirectoresTecnicos();
    Map<String, Integer> rankingNacionalidadesDT = seleccionDAO.getRankingNacionalidadesDirectoresTecnicos();

    // Crear tabla de selecciones por continente
    String[] columnNames = {"Continente", "Total Selecciones"};
    JTable table = new JTable(seleccionesPorContinente, columnNames);
    table.setRowHeight(30);

    // Convertir la tabla en texto
    StringBuilder tableText = new StringBuilder();
    for (int i = 0; i < table.getRowCount(); i++) {
        for (int j = 0; j < table.getColumnCount(); j++) {
            tableText.append(table.getValueAt(i, j)).append("\t");
        }
        tableText.append("\n");
    }

    // Crear texto para el ranking de nacionalidades de DT
    StringBuilder tabla2 = new StringBuilder();
    tabla2.append("Nacionalidades\t\tCantidad\n");
    for (Map.Entry<String, Integer> entry : rankingNacionalidadesDT.entrySet()) {
        tabla2.append(entry.getKey()).append("\t\t").append(entry.getValue()).append("\n");
    }

    // Crear el contenido del dashboard
    StringBuilder dashboardContent = new StringBuilder();
    dashboardContent.append("En esta sección, teniendo en cuenta los datos que fueron cargados en la matriz de selecciones\n")
            .append("se deben mostrar los siguientes datos:\n\n")
            .append("1. Total de selecciones Cargadas: ").append(totalSelecciones).append("\n")
            .append("2. Número de selecciones por continente:\n")
            .append(tableText.toString()) // Agregar la representación de la tabla
            .append("3. Cantidad de nacionalidades diferentes de los directores técnicos: ").append(cantidadNacionalidadesDT).append("\n")
            .append("4. Ranking de nacionalidades de directores técnicos:\n")
            .append(tabla2).append("\n");
            

    // Crear el panel con el texto
    JTextArea textArea = new JTextArea(dashboardContent.toString());
    textArea.setEditable(false);
    textArea.setLineWrap(true);
    textArea.setWrapStyleWord(true);

    // Agregar el panel al JScrollPane para habilitar la barra de desplazamiento vertical
    JScrollPane scrollPane = new JScrollPane(textArea);
    scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);

    // Establecer el diseño del jPanelMain como BorderLayout
    jPanelMain.setLayout(new BorderLayout());

    // Limpiar y agregar el JScrollPane al centro del panel principal
    jPanelMain.removeAll();
    jPanelMain.add(scrollPane, BorderLayout.CENTER);
    jPanelMain.repaint();
    jPanelMain.revalidate();
    
}



    /**
     * Función que se encarga de ajustar los elementos gráficos que componente
     * la opción de navegación de Dashboard de Resultados Define estilos,
     * etiquetas, iconos que decoran la opción del Menú. Esta opción de Menu
     * permite mostrar los diferentes datos que será extraidos de la información
     * de los resultados de los partidos que fueron cargados
     */
    private void pintarMenuDashboardRes() {
        btnDashboardRes.setIcon(new ImageIcon(getClass().getResource("/resources/icons/dashboard_resultados.png")));
        btnDashboardRes.setText("Dash Resultados");
        btnDashboardRes.setForeground(new java.awt.Color(255, 255, 255));

        JLabel vacioDashboardResultados = new JLabel();
        jPanelMenuDashboardRes.setBackground(new java.awt.Color(17, 41, 63));
        jPanelMenuDashboardRes.setPreferredSize((new java.awt.Dimension(220, 35)));
        jPanelMenuDashboardRes.setLayout(new BorderLayout(15, 0));
        jPanelMenuDashboardRes.add(vacioDashboardResultados, BorderLayout.WEST);
        jPanelMenuDashboardRes.add(btnDashboardRes, BorderLayout.CENTER);
        jPanelMenu.add(jPanelMenuDashboardRes);

        btnDashboardRes.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                System.out.println("Dashboard Resultados");
                accionDashboardRes();
            }
        });
    }

    /**
     * TRABAJO DEL ESTUDIANTE Se debe módificar este método para poder calcular
     * y pintar las diferentes informaciones que son solicitadas Revise el
     * proceso que se siguen en los demás métodos para poder actualizar la
     * información de los paneles
     */
    private void accionDashboardRes() {
    jLabelTop.setText("Dash De Resultados");
    JPanel paneles = new JPanel();
     paneles.setLayout(new GridLayout(2, 2, 5, 5));
      paneles.setPreferredSize((new java.awt.Dimension(620, 410)));
       paneles.setMaximumSize( jPanelRight.getPreferredSize());
    int uno = seleccionDAO.getTotalResultadosCargadas();
    double dos = seleccionDAO.getPromedioGolesPorPartido();
    String tres1 = seleccionDAO.getPartidoConMasGoles();
    String tres2 = seleccionDAO.getPartidoConMenosGoles();
    int cuatro1 = seleccionDAO.getNumeroPartidosConGanador();
    int cuatro2 = seleccionDAO.getNumeroPartidosConEmpatados();
    SeleccionDAO.EquipoConGoles cinco1 = seleccionDAO.getEquipoConMasGoles();
    SeleccionDAO.EquipoConGoles cinco2 = seleccionDAO.getEquipoConMenosGoles();
    String seis1 = seleccionDAO.getEquipoConMasPuntos();
    String seis2 = seleccionDAO.getEquipoConMenosPuntos();
    String siete1 = seleccionDAO.getContinenteConMasGoles();
    String siete2 = seleccionDAO.getContinenteConMenosGoles();
    
    // Obtener los equipos clasificados por grupo
    List<String> equiposClasificados = seleccionDAO.getEquiposClasificadosPorGrupo();
        System.out.println(equiposClasificados.size());
    JTextArea a = new JTextArea();
    a.setText("En esta seccion, teniendo en cuenta los datos que fueron cargados en la matriz de resultados\n" +
            "se deben mostrar los siguientes datos:\n" +
            "\n" +
            "1. Numero de partidos cargados: " + uno + "\n" +
            "2. Promedio de goles por partido: " + dos + "\n" +
            "3. Partido con mas goles: " + tres1 + "\n" +
            "   y partido con menos goles: " + tres2 + "\n" +
            "4. Numero de partidos donde hubo un ganador: " + cuatro1 + " y numero de partidos donde hubo empate: " + cuatro2 + "\n" +
            "5. Seleccion mas goles: " + cinco1 + " y con menos goles: " + cinco2 + "\n" +
            "6. Seleccion con mas puntos: " + seis1 + " y menos puntos: " + seis2 + "\n" +
            "7. Continente mas goles: " + siete1 + " y menos goles: " + siete2 + "\n" +
            "8. Clasificados por cada grupo (Clasifican los dos primeros equipos de cada grupo):\n");

    // Agregar los equipos clasificados por grupo al texto
    for (String equipo : equiposClasificados) {
        a.append("- " + equipo + "\n");
    }
//uno
    JPanel PuntoUno = new JPanel();
        PuntoUno.setLayout(new BoxLayout(PuntoUno, BoxLayout.Y_AXIS));
        PuntoUno.add(crearGraficaCircularRES(uno, "Proporción de partidos cargados"));
    //JScrollPane scrollPane = new JScrollPane(a);
    //scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
//dos - Jhice
//cuatro     
 JPanel PuntoCuatro = new JPanel();
        PuntoCuatro.setLayout(new BoxLayout(PuntoCuatro, BoxLayout.Y_AXIS));
        PuntoCuatro.add(crearGraficaBarrasAgrupadasRES(cuatro1, cuatro2));
        
//cinco 
JPanel PuntoCinco = new JPanel();
        PuntoCinco.setLayout(new BoxLayout(PuntoCinco, BoxLayout.Y_AXIS));
        PuntoCinco.add(crearGraficaBarrasApiladas(cinco1, cinco2));
       
        paneles.add(PuntoCinco);
        paneles.add(PuntoUno);
        paneles.add(PuntoCuatro);
        paneles.add(PuntoCinco);
    
    
        jPanelMain.setLayout(new BorderLayout());
        
    jPanelMain.removeAll();
    //jPanelMain.add(scrollPane,BorderLayout.CENTER);
    jPanelMain.add(paneles,BorderLayout.PAGE_START);

    jPanelMain.repaint();
    jPanelMain.revalidate();
}
private ChartPanel crearGraficaCircularRES(int totalCargadas, String nombreGrafico) {
   int PARTIDOS=64;
   int restantes=PARTIDOS - totalCargadas;
    DefaultPieDataset dataset = new DefaultPieDataset();
    dataset.setValue("Cargadas: "+totalCargadas+" selecciones", totalCargadas);
    dataset.setValue("Restantes: "+restantes+" selecciones", PARTIDOS - totalCargadas);

    JFreeChart graficoCircular = ChartFactory.createPieChart(
            nombreGrafico,
            dataset,
            true,
            true,
            false
    );

    PiePlot plot = (PiePlot) graficoCircular.getPlot();
    plot.setSimpleLabels(true);

    ChartPanel panel = new ChartPanel(graficoCircular);
    panel.setMouseWheelEnabled(true);
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    return panel;
}

private ChartPanel crearGraficaBarrasAgrupadasRES(int numeroPartidosConGanador, int numeroPartidosConEmpate) {
    DefaultCategoryDataset dataset = new DefaultCategoryDataset();
    dataset.addValue(numeroPartidosConGanador, "Ganados", "");
    dataset.addValue(numeroPartidosConEmpate, "Empatados", "");

    JFreeChart graficoBarras = ChartFactory.createBarChart(
            "Número de partidos: Ganados o Empatados",
            "",
            "Cantidad",
            dataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
    );

    CategoryPlot plot = graficoBarras.getCategoryPlot();
    BarRenderer renderer = (BarRenderer) plot.getRenderer();
    renderer.setSeriesPaint(0, new Color(9, 72, 132));
    renderer.setSeriesPaint(1, new Color(18, 119, 217));

    CategoryAxis axis = plot.getDomainAxis();
    axis.setTickLabelsVisible(false);

    ChartPanel panel = new ChartPanel(graficoBarras);
    panel.setMouseWheelEnabled(true);
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    return panel;
}

private ChartPanel crearGraficaBarrasApiladas(EquipoConGoles equipoConMasGoles, EquipoConGoles equipoConMenosGoles) {
   System.out.println("Equipo con más goles: " + equipoConMasGoles.getNombreEquipo()+ " - " + equipoConMasGoles.getCantidadGoles());
System.out.println("Equipo con menos goles: " + equipoConMenosGoles.getNombreEquipo()+ " - " + equipoConMenosGoles.getCantidadGoles());

    DefaultCategoryDataset dataset = new DefaultCategoryDataset();
    dataset.addValue(equipoConMasGoles.getCantidadGoles(), "Goles", equipoConMasGoles.getNombreEquipo());
    dataset.addValue(equipoConMenosGoles.getCantidadGoles(), "Goles", equipoConMenosGoles.getNombreEquipo());

    JFreeChart graficoBarras = ChartFactory.createBarChart(
            "Selecciones con más y menos goles",
            "Selección",
            "Cantidad de goles",
            dataset,
            PlotOrientation.HORIZONTAL,
            true,
            true,
            false
    );

    CategoryPlot plot = graficoBarras.getCategoryPlot();
    BarRenderer renderer = (BarRenderer) plot.getRenderer();
    renderer.setSeriesPaint(0, new Color(9, 72, 132));
    renderer.setSeriesPaint(1, new Color(18, 119, 217));
    
    CategoryAxis domainAxis = plot.getDomainAxis();
    domainAxis.setTickLabelsVisible(true);
    
    // Ajustar el espacio entre las barras
    double margin = 0.2; // Espacio entre las barras (20% del ancho total)
    domainAxis.setCategoryMargin(margin);
    
    // Ajustar el espacio en los extremos del eje
    double start = margin / 2.0; // Espacio al principio del eje
    double end = 1.0 - margin / 2.0; // Espacio al final del eje
    domainAxis.setLowerMargin(start);
    domainAxis.setUpperMargin(1.0 - end);
    
    ChartPanel panel = new ChartPanel(graficoBarras);
    panel.setMouseWheelEnabled(true);
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    return panel;
}






    /**
     * Función que permite darle estilos y agregar los componentes gráficos del
     * contendor de la parte izquierda de la interfaz, dónde se visulaiza el
     * menú de navegaación
     */
    private void pintarPanelIzquierdo() {
        // Se elimina el color de fondo del panel del menú
        jPanelMenu.setOpaque(false);

        // Se agrega un border izquierdo, de color blanco para diferenciar el panel de menú del panel de contenido
        jPanelLeft.setBorder(BorderFactory.createMatteBorder(0, 0, 0, 3, Color.WHITE));

        // Se define un BoxLayot de manera vertical para los elementos del panel izquierdo
        jPanelLeft.setLayout(new BoxLayout(jPanelLeft, BoxLayout.Y_AXIS));
        jPanelLeft.setBackground(new java.awt.Color(0, 24, 47));
        getContentPane().add(jPanelLeft, java.awt.BorderLayout.LINE_START);
        jPanelLeft.add(jPanelMenu);
        jPanelLeft.setPreferredSize((new java.awt.Dimension(220, 540)));
        jPanelLeft.setMaximumSize(jPanelLeft.getPreferredSize());
    }

    /**
     * Función que permite leer un archivo y procesar el contenido que tiene en
     * cada una de sus líneas El contenido del archivo es procesado y cargado en
     * la matriz de selecciones. Una vez la información se carga en la atriz, se
     * hace un llamado a la función pintarTablaSelecciones() que se encarga de
     * pintar en la interfaz una tabla con la información almacenada en la
     * matriz de selecciones
     */
    public void cargarFileSelecciones() {

        JFileChooser cargarFile = new JFileChooser();
        cargarFile.showOpenDialog(cargarFile);

        Scanner entrada = null;
        try {

            // Se obtiene la ruta del archivo seleccionado
            String ruta = cargarFile.getSelectedFile().getAbsolutePath();

            // Se obtiene el archivo y se almancena en la variable f
            File f = new File(ruta);
            entrada = new Scanner(f);

            // Permite que el sistema se salte la léctura de los encabzados del archivo CSV
            entrada.nextLine();

            // Se leen cada unas de las líneas del archivo
            while (entrada.hasNext()) {
                String line = entrada.nextLine();
                String[] columns = line.split(",");

                Seleccion seleccion = new Seleccion(columns[1], columns[2], columns[3], columns[4]);
                if (seleccionDAO.registrarSeleccion(seleccion)) {
                    System.out.println("Seleccion " + seleccion.getNombre() + " registrada");
                } else {
                    System.out.println("Error " + seleccion.getNombre());
                }
            }

            pintarTablaSelecciones();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (entrada != null) {
                entrada.close();
            }
        }
    }

    /**
     * Función que se encarga de pinta la tabla con la información de las
     * selelceciones que fue cargada previamente La tabla tiene definido un
     * encabezado con las siguentes columnas: {"ID","Selección", "Continente",
     * "DT", "Nacionalidad DT"} Columnas que se corresponden son la información
     * que fue leida desde el archivo csv
     */
    public void pintarTablaSelecciones() {

        String[] columnNames = {"Selección", "Continente", "DT", "Nacionalidad DT"};
        JTable table = new JTable(selecciones, columnNames);
        table.setRowHeight(30);

        JPanel form = new JPanel();
        form.setLayout(new GridLayout(4, 1, 0, 0));

        JLabel label = new JLabel();
        label.setText("Busqueda de Equipos");
        form.add(label);

        JTextField field = new JTextField();
        form.add(field);

        JPanel panelBotones = new JPanel();
        panelBotones.setLayout(new GridLayout(1, 2, 30, 0));

        JButton buscar = new JButton();
        buscar.setText("Buscar");
        panelBotones.add(buscar);
        buscar.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                System.out.println("Buscar");
                accionBuscar(field);
            }
        });
        //hacer que funcioneeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee

        JButton limpiar = new JButton();
        limpiar.setText("Ver Todos");
        panelBotones.add(limpiar);
        form.add(panelBotones);
        //hacer que funcioneeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee

        JPanel seleccionesPanel = new JPanel();
        seleccionesPanel.setLayout(new BoxLayout(seleccionesPanel, BoxLayout.Y_AXIS));
        seleccionesPanel.setPreferredSize((new java.awt.Dimension(620, 410)));
        seleccionesPanel.setMaximumSize(jPanelRight.getPreferredSize());

        JScrollPane scrollPane = new JScrollPane(table);
        seleccionesPanel.add(form);
        seleccionesPanel.add(scrollPane);

        jPanelMain.removeAll();
        jPanelMain.add(seleccionesPanel, BorderLayout.PAGE_START);
        jPanelMain.repaint();
        jPanelMain.revalidate();
    }

    public void accionBuscar(JTextField field) {
        String equipo = field.getText();
        seleccionDAO.buscarSeleccion(equipo);
        seleccionDAO.getSeleccionesMatriz(equipo);
        pintarTablaBuscar(equipo);
    }

    public void pintarTablaBuscar(String valor) {

        String[] columnNames = {"Selección", "Continente", "DT", "Nacionalidad DT"};
        String[][] data = seleccionDAO.getSeleccionesMatriz(valor);

        JTable table = new JTable(data, columnNames);
        table.setRowHeight(30);

        JPanel form = new JPanel();
        form.removeAll();
        form.setLayout(new GridLayout(4, 1, 0, 0));

        JLabel label = new JLabel();
        label.setText("Busqueda de Equipos");
        form.add(label);

        JTextField field = new JTextField();
        form.add(field);

        JPanel panelBotones = new JPanel();
        panelBotones.setLayout(new GridLayout(1, 2, 30, 0));

        JButton buscar = new JButton();
        buscar.setText("Buscar");
        panelBotones.add(buscar);
        buscar.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                System.out.println("Buscar");
                accionBuscar(field);
            }
        });

        JButton limpiar = new JButton();
        limpiar.setText("Ver Todos");
        panelBotones.add(limpiar);
        form.add(panelBotones);

        JPanel seleccionesPanel = new JPanel();
        seleccionesPanel.setLayout(new BoxLayout(seleccionesPanel, BoxLayout.Y_AXIS));
        seleccionesPanel.setPreferredSize((new java.awt.Dimension(620, 410)));
        seleccionesPanel.setMaximumSize(jPanelRight.getPreferredSize());

        JScrollPane scrollPane = new JScrollPane(table);
        seleccionesPanel.add(form);
        seleccionesPanel.add(scrollPane);

        jPanelMain.removeAll();
        jPanelMain.add(seleccionesPanel, BorderLayout.PAGE_START);
        jPanelMain.repaint();
        jPanelMain.revalidate();
    }

    /**
     * Función que tiene la lógica que permite leer un archivo CSV de resultados
     * y cargarlo sobre la matriz resultados que se tiene definida cómo variable
     * global. Luego de cargar los datos en la matriz, se llama la función
     * pintarTablaResultados() que se encarga de visulizar el contenido de la
     * matriz en un componente gráfico de tabla
     */
    public void cargarFileResultados() {

        JFileChooser cargarFile = new JFileChooser();
        cargarFile.showOpenDialog(cargarFile);

        Scanner entrada = null;
        try {
            // Se obtiene la ruta del archivo seleccionado
            String ruta = cargarFile.getSelectedFile().getAbsolutePath();

            // Se obtiene el archivo y se almancena en la variable f
            File f = new File(ruta);
            entrada = new Scanner(f);

            // Se define las dimensiones de la matriz de selecciones
            resultados = new String[48][7];
            entrada.nextLine();

            int i = 0;
            // Se iteran cada una de las líneas del archivo
            while (entrada.hasNext()) {
                System.out.println(i);
                String line = entrada.nextLine();
                String[] columns = line.split(",");

                for (int j = 0; j < columns.length; j++) {
                    resultados[i][j] = columns[j];
                }
                i++;
            }

            pintarTablaResultados();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            if (entrada != null) {
                entrada.close();
            }
        }
    }

    /**
     * Función que se encarga de pintar la tabla con la información de los
     * resultados que fue cargada previamente La tabla tiene definido un
     * encabezado con las siguentes columnas: {"Grupo","Local", "Visitante",
     * "Continente L", "Continente V", "Goles L", "Goles V"} Columnas que se
     * corresponden son la información que fue leida desde el archivo csv
     */
    public void pintarTablaResultados() {

        String[] columnNames = {"Grupo", "Local", "Visitante", "Continente L", "Continente V", "Goles L", "Goles V"};
        JTable table = new JTable(resultados, columnNames);
        table.setRowHeight(30);

        JPanel form = new JPanel();
        form.setLayout(new GridLayout(4, 1, 0, 0));

        JLabel label = new JLabel();
        label.setText("Busqueda de Resultados");
        form.add(label);

        JTextField field = new JTextField();
        form.add(field);

        JPanel panelBotones = new JPanel();
        panelBotones.setLayout(new GridLayout(1, 2, 30, 0));

        JButton buscar = new JButton();
        buscar.setText("Buscar");
        panelBotones.add(buscar);

        JButton limpiar = new JButton();
        limpiar.setText("Ver Todos");
        panelBotones.add(limpiar);
        form.add(panelBotones);

        JPanel seleccionesPanel = new JPanel();
        seleccionesPanel.setLayout(new BoxLayout(seleccionesPanel, BoxLayout.Y_AXIS));
        seleccionesPanel.setPreferredSize((new java.awt.Dimension(620, 410)));
        seleccionesPanel.setMaximumSize(jPanelRight.getPreferredSize());

        JScrollPane scrollPane = new JScrollPane(table);
        seleccionesPanel.add(form);
        seleccionesPanel.add(scrollPane);

        jPanelMain.removeAll();
        jPanelMain.add(seleccionesPanel, BorderLayout.PAGE_START);
        jPanelMain.repaint();
        jPanelMain.revalidate();
    }

    /**
     * Función que permite darle estilos y agregar los componentes gráficos del
     * contendor de la parte derecha de la interfaz, dónde se visulaiza de
     * manera dinámica el contenido de cada una de las funciones que puede
     * realizar el usuario sobre la aplicación.
     */
    private void pintarPanelDerecho() {

        // Define las dimensiones del panel
        jPanelMain.setPreferredSize((new java.awt.Dimension(620, 420)));
        jPanelMain.setMaximumSize(jPanelLabelTop.getPreferredSize());

        getContentPane().add(jPanelRight, java.awt.BorderLayout.CENTER);
        jPanelRight.setLayout(new FlowLayout(FlowLayout.CENTER, 0, 0));
        jPanelRight.add(jPanelLabelTop, BorderLayout.LINE_START);
        jPanelRight.add(jPanelMain);
        jPanelRight.setPreferredSize((new java.awt.Dimension(620, 540)));
        jPanelRight.setMaximumSize(jPanelRight.getPreferredSize());
    }

    /**
     * Función que permite pinta la barra azul del contenedor de contenidos.
     * Barra azul que permite indicar en que sección que se encuentra navegando
     * el usuario.
     */
    private void pintarLabelTop() {
        jLabelTop = new JLabel();
        jLabelTop.setFont(new java.awt.Font("Liberation Sans", 1, 36)); // NOI18N
        jLabelTop.setForeground(new java.awt.Color(241, 241, 241));
        jLabelTop.setText("Home");

        JLabel vacioTopLabel = new JLabel();
        jPanelLabelTop.setLayout(new BorderLayout(15, 0));
        jPanelLabelTop.add(vacioTopLabel, BorderLayout.WEST);
        jPanelLabelTop.setBackground(new java.awt.Color(18, 119, 217));
        jPanelLabelTop.add(jLabelTop, BorderLayout.CENTER);
        jPanelLabelTop.setPreferredSize((new java.awt.Dimension(620, 120)));
        jPanelLabelTop.setMaximumSize(jPanelLabelTop.getPreferredSize());
    }

    public static void main(String args[]) {

        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new GUIManual().setVisible(true);
            }
        });
    }
}
